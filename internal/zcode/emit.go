// emit.go 入站转换的插件侧出口：把 sse.go 产出的 OpenAI Chat SSE 解析成
// chat.generate 的增量事件（text.delta / reasoning.delta / tool_call.delta / usage / finish_reason）。
//
// 为什么不把 Anthropic 帧直接 emit：sse.go 里的转换器已经处理了本渠道最脏的三件事
// （signature_delta 忽略、thinking→reasoning_content、截断/空响应自证），
// 它的输出是标准 OpenAI SSE。在它**之后**再解析一层，等于复用旧版全部实测结论，
// 且新增代码只有「SSE 行 → 事件」这一层纯映射，没有第二份方言逻辑。
//
// 旧版这里是 `upstream.StreamCapture` / `upstream.Aggregate`（core 内的统一规范化层）；
// 插件化后 core 不再共享代码，故这层搬进插件。
package zcode

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"

	"wild-work3/zcode-ext/internal/protocol/chat"
	"wild-work3/zcode-ext/internal/protocol/errkind"
)

// Emit 是 plugin.Emit 的对等签名（避免 zcode 包反向依赖 protocol/plugin）。
type Emit func(event string, payload any) error

// StreamChat 消费上游 Anthropic SSE，边转边 emit，返回用量与 finish_reason。
//
// 返回的 error 已按 errkind 归类（IdleTimeout / Truncated / Cancel），
// 由 channel 层转成 JSONL 错误对象；emit 本身失败（下游断开）会原样返回。
func StreamChat(r io.Reader, model string, emit Emit) (*chat.Usage, string, error) {
	src := newAnthropicStream(r, model)
	sc := bufio.NewScanner(src)
	sc.Buffer(make([]byte, 0, 64*1024), 4<<20)

	var (
		usage  chat.Usage
		finish string
		start  = time.Now()
		first  bool
	)
	// markFirst 记录首字耗时（首帧增量到达即定），用于 stats/前端「首字」展示。
	markFirst := func() {
		if !first {
			first = true
			usage.FirstTokenMs = time.Since(start).Milliseconds()
		}
	}
	defer func() { usage.TimeMs = time.Since(start).Milliseconds() }()
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 || !bytes.HasPrefix(line, []byte("data:")) {
			continue
		}
		payload := bytes.TrimSpace(line[len("data:"):])
		if len(payload) == 0 {
			continue
		}
		if bytes.Equal(payload, []byte("[DONE]")) {
			break
		}
		var chunk struct {
			Choices []struct {
				Delta struct {
					Content          string `json:"content"`
					ReasoningContent string `json:"reasoning_content"`
					ToolCalls        []struct {
						Index    int    `json:"index"`
						ID       string `json:"id"`
						Type     string `json:"type"`
						Function struct {
							Name      string `json:"name"`
							Arguments string `json:"arguments"`
						} `json:"function"`
					} `json:"tool_calls"`
				} `json:"delta"`
				FinishReason string `json:"finish_reason"`
			} `json:"choices"`
			Usage *struct {
				PromptTokens        int `json:"prompt_tokens"`
				CompletionTokens    int `json:"completion_tokens"`
				TotalTokens         int `json:"total_tokens"`
				PromptTokensDetails *struct {
					CachedTokens     int  `json:"cached_tokens"`
					CacheWriteTokens *int `json:"cache_write_tokens"`
				} `json:"prompt_tokens_details"`
				CompletionTokensDetails *struct {
					ReasoningTokens int `json:"reasoning_tokens"`
				} `json:"completion_tokens_details"`
			} `json:"usage"`
		}
		if err := json.Unmarshal(payload, &chunk); err != nil {
			continue
		}
		for _, ch := range chunk.Choices {
			if t := ch.Delta.Content; t != "" {
				markFirst()
				if err := emit("text.delta", map[string]any{"content": t}); err != nil {
					return nil, "", err
				}
			}
			if t := ch.Delta.ReasoningContent; t != "" {
				markFirst()
				if err := emit("reasoning.delta", map[string]any{"content": t}); err != nil {
					return nil, "", err
				}
			}
			for _, tc := range ch.Delta.ToolCalls {
				markFirst()
				ev := map[string]any{
					"index":              float64(tc.Index),
					"id":                 tc.ID,
					"type":               tc.Type,
					"name":               tc.Function.Name,
					"arguments_fragment": tc.Function.Arguments,
				}
				if err := emit("tool_call.delta", ev); err != nil {
					return nil, "", err
				}
			}
			if ch.FinishReason != "" {
				finish = ch.FinishReason
			}
		}
		if u := chunk.Usage; u != nil {
			usage.PromptTokens = u.PromptTokens
			usage.CompletionTokens = u.CompletionTokens
			usage.TotalTokens = u.TotalTokens
			// 来源标记：值来自上游同一 usage 对象，明确 0 也是有效实报（W3-34）。
			usage.MarkReported()
			// 缓存两维**各自**判定上报状态：上游只给 cached 不给 write 时，
			// write 是「未提供」而不是「明确 0」，不能一起置 true（面板会谎报 0% 写入）。
			if u.PromptTokensDetails != nil {
				if usage.Cache == nil {
					usage.Cache = &chat.CacheUsage{}
				}
				usage.Cache.Cached = u.PromptTokensDetails.CachedTokens
				usage.Cache.CachedReported = true
				if p := u.PromptTokensDetails.CacheWriteTokens; p != nil {
					usage.Cache.Write = *p
					usage.Cache.WriteReported = true
				}
			}
			_ = u.CompletionTokensDetails // 思考 token 是 output 子集，不另计
		}
	}
	if err := sc.Err(); err != nil {
		if errors.Is(err, ErrIdleTimeout) {
			return nil, "", &Error{Kind: errkind.IdleTimeout,
				Msg: "zcode：上游流空闲超时（连续无数据），已中止"}
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, "", &Error{Kind: errkind.Cancel, Msg: "请求已取消"}
		}
		msg := strings.TrimSpace(err.Error())
		// sse.go 的 finalize 会自证两类异常，二者处置不同：
		//   - 「没有任何内容块」是上游**成功回了 200 但什么也没给**（实测常见于模型/通道
		//     不匹配），换号/重试有意义 ⇒ Server；
		//   - 「块中途结束 / 读取中断」是流被截断，已吐出的内容不完整 ⇒ Truncated。
		if strings.Contains(msg, "空响应") {
			return nil, "", &Error{Kind: errkind.Server, Msg: msg}
		}
		// 空闲看门狗：sse.go 的 abnormal 会把 ErrIdleTimeout **包成文案**（它无法
		// 把哨兵错误穿出来），故这里按文案归一，否则会被误当成「截断」而不触发换号。
		if strings.Contains(msg, "空闲超时") {
			return nil, "", &Error{Kind: errkind.IdleTimeout, Msg: msg}
		}
		return nil, "", &Error{Kind: errkind.Truncated, Msg: msg}
	}
	return &usage, finish, nil
}
