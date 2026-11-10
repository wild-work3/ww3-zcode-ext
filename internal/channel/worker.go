package channel

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"wild-work3/zcode-ext/internal/protocol/chat"
	"wild-work3/zcode-ext/internal/protocol/errkind"
	"wild-work3/zcode-ext/internal/protocol/plugin"
	"wild-work3/zcode-ext/internal/zcode"
)

// generate 实现 chat.generate 流式对话。
//
// 十步顺序（契约 protocol/docs/IPC_CHAT.md，改动前先读）：
//  1. 解 GenerateRequest；
//  2. cfg = channel_config 覆盖 config.json（**AGENTS 陷阱 20**）；
//  3. 按 account_uid / cfg.auth / 列表顺序取凭据；
//  4. 组装上游请求体（extra 透传 + 必要字段覆写）；
//  5. 出站转换（OpenAI Chat → Anthropic Messages，含身份块准入）；
//  6. **先判 rc == nil 再 defer rc.Close()**（AGENTS 陷阱 21）；
//  7. 逐帧 emit 增量事件；
//  8. 收尾 finish_reason / usage / completed；
//  9. 写统计；
//  10. 返回 usage 作为 result。
func (d *Daemon) generate(ctx context.Context, raw Raw, emit plugin.Emit) (Raw, *Err) {
	var req chat.GenerateRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return nil, errf(errkind.BadParams, errkind.ScopeRequest, "请求体解析失败: %v", err)
	}
	cfg := channelConfig(raw)
	cfg = mergeConfig(cfg, req)

	// 未知模型**本地拒绝**：本渠道的通道白名单与思考档位都按模型 id 判定，
	// 让一个不认识的 id 打到上游只会换回 405/3012 或空响应，且可能记风控冷却。
	model := zcode.TrimModel(req.Model)
	if model == "" {
		return nil, errf(errkind.BadParams, errkind.ScopeRequest, "缺少 model")
	}
	if !zcode.KnownModel(model) {
		return nil, errf(errkind.ModelBlocked, errkind.ScopeModel,
			"未知模型 %s（本渠道仅支持静态表内的模型，可用 models.list 查询）", req.Model)
	}

	a, rerr := d.account(cfg, req.AccountUID)
	if rerr != nil {
		return nil, errf(rerr.Kind, scopeOfKind(rerr.Kind), "%s", rerr.Msg)
	}
	client := d.client(cfg)

	body, err := buildUpstreamBody(&req, model)
	if err != nil {
		return nil, errf(errkind.BadParams, errkind.ScopeRequest, "构造上游请求失败: %v", err)
	}

	start := time.Now()
	rc, status, respBody, cerr := client.ChatStream(ctx, a, body)
	if cerr != nil {
		if errors.Is(cerr, context.Canceled) || errors.Is(cerr, context.DeadlineExceeded) || ctx.Err() != nil {
			return nil, errf(errkind.Cancel, errkind.ScopeRequest, "请求已取消")
		}
		e := zcodeErr(cerr, errkind.Server)
		d.recordChat(cfg, a, req.Model, false, e.Kind, nil, 0, time.Since(start))
		return nil, errf(e.Kind, scopeOfKind(e.Kind), "上游请求失败: %s", e.Msg)
	}
	// ⚠ AGENTS 陷阱 21：**先判 nil 再 defer**。rc 为 nil 时 defer 一个 nil 接口会 panic；
	// 反过来（先 defer 再判）在这类「错误通过返回值而非 error 传递」的客户端上必炸。
	if rc == nil {
		kind := zcode.Classify(status, string(respBody))
		e := zcodeErr(errFromStatus(status, respBody), kind)
		d.recordChat(cfg, a, req.Model, false, kind, nil, 0, time.Since(start))
		return nil, errf(kind, scopeOfKind(kind), "上游返回 %d: %s", status, e.Msg)
	}
	defer rc.Close()

	usage, finish, perr := zcode.StreamChat(rc, req.Model, zcode.Emit(emit))
	if perr != nil {
		if errors.Is(perr, context.Canceled) || ctx.Err() != nil {
			return nil, errf(errkind.Cancel, errkind.ScopeRequest, "请求已取消")
		}
		e := zcodeErr(perr, errkind.Server)
		if errors.Is(perr, zcode.ErrIdleTimeout) {
			e = &zcode.Error{Kind: errkind.IdleTimeout, Msg: perr.Error()}
		}
		d.recordChat(cfg, a, req.Model, false, e.Kind, usage, 0, time.Since(start))
		return nil, errf(e.Kind, scopeOfKind(e.Kind), "流式读取失败: %s", e.Msg)
	}

	if finish == "" {
		finish = "stop"
	}
	_ = emit("finish_reason", map[string]any{"reason": finish})
	if usage == nil {
		usage = &chat.Usage{}
	}
	if usage.TimeMs == 0 {
		usage.TimeMs = time.Since(start).Milliseconds()
	}
	_ = emit("usage", usage)
	_ = emit("completed", map[string]any{})

	d.recordChat(cfg, a, req.Model, true, "", usage, int(usage.FirstTokenMs), time.Since(start))
	return mustJSON(usage), nil
}

// buildUpstreamBody 组装发给上游的请求体（**OpenAI Chat 形状**）。
//
// 注意分工：这里只负责「把归一化请求摊平成上游要看的字段」；
// 真正的方言投影（→ Anthropic Messages + 身份块）在 zcode.buildMessagesBody 里，
// 由 Client.ChatStream 调用。分两层是因为旧版 messages.go 已经把这层逻辑
// 实测到 752 行（图片、tool_result、缓存断点、effort 归一），不该在插件层重写。
func buildUpstreamBody(req *chat.GenerateRequest, model string) ([]byte, error) {
	m := map[string]any{}
	if len(req.Extra) > 0 {
		if err := json.Unmarshal(req.Extra, &m); err != nil {
			return nil, err
		}
	}
	// 协议内部字段不得透传到上游。
	delete(m, "channel_config")
	delete(m, "account_uid")
	delete(m, "conversation_id")
	// model 用**去前缀**的上游名（`zcode/GLM-5.3-Flash` → `GLM-5.3-Flash`）；
	// buildMessagesBody 里还会再兜一次前缀，这里先归一让通道选路与目录匹配都正确。
	m["model"] = model
	m["messages"] = req.Messages
	m["stream"] = true
	if len(req.Tools) > 0 {
		m["tools"] = req.Tools
	}
	if req.ToolChoice != nil {
		m["tool_choice"] = req.ToolChoice
	}
	if req.Temperature != nil {
		m["temperature"] = *req.Temperature
	}
	if req.MaxTokens != nil {
		m["max_tokens"] = *req.MaxTokens
	}
	// 思考档位：本渠道只有 output_config.effort 三档，由 buildMessagesBody 的
	// declaredEffort 决定是否下发（未知模型不下发）。这里只透传原始值。
	if req.ReasoningEffort != "" {
		m["reasoning_effort"] = req.ReasoningEffort
	}
	return json.Marshal(m)
}

// errFromStatus 把上游非 200 响应转为可分类错误。
func errFromStatus(status int, body []byte) error {
	msg := string(body)
	var env struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Msg     string `json:"msg"`
	}
	if json.Unmarshal(body, &env) == nil {
		if env.Message != "" {
			msg = env.Message
		} else if env.Msg != "" {
			msg = env.Msg
		}
	}
	return &zcode.Error{Kind: zcode.Classify(status, msg), Status: status, Msg: msg}
}

// recordChat 记录一次对话统计（fail-open）。
func (d *Daemon) recordChat(cfg zcode.Config, a *zcode.Auth, model string, ok bool, kind errkind.Kind, usage *chat.Usage, firstTokenMs int, dtime time.Duration) {
	if d.stat == nil {
		return
	}
	rec := statRecord{
		Kind:  "chat",
		OK:    ok,
		Model: model,
		Time:  dtime,
	}
	if a != nil {
		rec.Account = a.UID
		rec.Region = a.RegionOf()
	}
	if kind != "" {
		rec.ErrKind = kind
	}
	rec.FirstTokenMs = firstTokenMs
	if usage != nil {
		rec.PromptTokens = usage.PromptTokens
		rec.CompletionTokens = usage.CompletionTokens
		rec.TotalTokens = usage.TotalTokens
		rec.PromptReported = usage.PromptReported
		rec.CompletionReported = usage.CompletionReported
		rec.TotalReported = usage.TotalReported
		if usage.Cache != nil {
			rec.CachedTokens = usage.Cache.Cached
			rec.CachedReported = usage.Cache.CachedReported
		}
		if rec.FirstTokenMs == 0 && usage.FirstTokenMs > 0 {
			rec.FirstTokenMs = int(usage.FirstTokenMs)
		}
	} else {
		rec.PromptTokens = -1
		rec.CompletionTokens = -1
		rec.TotalTokens = -1
	}
	d.appendStat(rec, cfg)
}
