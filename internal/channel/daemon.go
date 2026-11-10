// Package channel 是 zcode-ext（智谱 ZCode / z.ai）渠道插件的实现：
// 控制面 + 会话面同一份方法表。
//
// 运行形态（与 raccoon-ext / workbuddy-ext 一致）：`call` 一次性
// （stdin 一条 invoke → 事件 + result → 退出）与 `stdio` 常连；core 一律用 call，
// 无常驻进程 ⇒ 登录流程状态必须落盘（见 internal/zcode/flows.go 的 W3-20 注释）。
package channel

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"wild-work3/zcode-ext/internal/protocol/authflow"
	"wild-work3/zcode-ext/internal/protocol/chat"
	"wild-work3/zcode-ext/internal/protocol/cli"
	"wild-work3/zcode-ext/internal/protocol/errkind"
	"wild-work3/zcode-ext/internal/protocol/meta"
	"wild-work3/zcode-ext/internal/protocol/plugin"
	"wild-work3/zcode-ext/internal/protocol/stat"
	"wild-work3/zcode-ext/internal/zcode"
)

// PluginID 是插件 ID（模型命名空间前缀同为 zcode）。
const PluginID = "zcode"

// Log 是插件进程级日志器（main 里初始化；nil 时静默）。
var Log *cli.Logger

// Manifest 返回插件清单。
//
// ⚠ 能力表里**没有** `accounts.checkin`：本渠道的积分领取
// （`POST /api/v1/zcode-plan/billing/claim`）依赖阿里云验证码网页 SDK + headful
// chromium，且每个 plan 需现 mint 一次性 captcha param，Go 侧无法纯静态移植
// （见 internal/zcode/constants.go 包注释第 1 条）。**刻意不声明**而不是声明后失败：
// core 会按能力表决定是否展示「签到」入口，声明了就必然走到一条假路径。
func Manifest() meta.Manifest {
	return meta.Manifest{
		ID:              PluginID,
		Name:            "智谱 ZCode（z.ai）",
		DefaultChannels: []meta.DefaultChannel{{ID: PluginID, Name: "ZCode", Config: map[string]any{}}},
		Version:         "1.0.0",
		Kind:            "channel",
		Protocol:        meta.ProtocolRng{Min: 1, Max: 1},
		Execution: meta.Execution{
			Control:      plugin.ModeCall,
			Conversation: plugin.ModeCall,
			// 上游是积分制 start-plan，账号级并发有限；4 与其它渠道插件一致。
			MaxWorkers:         4,
			IdleTimeoutSeconds: 0,
		},
		Capabilities: meta.Capabilities{
			Control: []string{
				"models.list", "config.schema", "config.get", "config.set",
				"auth.methods", "auth.begin", "auth.status", "auth.finish", "auth.cancel",
				"accounts.list", "accounts.resource_detail", "accounts.refresh",
				"credits.get", "stats.get", "health.check", "ping",
			},
			Conversation: []string{"chat.generate"},
		},
		Icon:  "zcode",
		Color: "#3b82f6",
		Description: "智谱 ZCode（z.ai）渠道：device_poll 设备授权登录（zcode_jwt），" +
			"Anthropic Messages 形态出入站转换，模型前缀 zcode。",
	}
}

// Daemon 是控制面状态（凭据仓库 + 登录流程目录 + 统计）。
type Daemon struct {
	dataDir string
	store   *zcode.Store
	login   *zcode.LoginManager
	stat    *stat.Store
}

// NewDaemon 创建控制面（dataDir 由 core 通过 --data 注入）。
func NewDaemon(dataDir string) *Daemon {
	st := zcode.NewStore(dataDir)
	// 模型目录日志走统一 logger（旧版是 log.Printf，插件里 stderr 被 core 当诊断流）。
	zcode.Logf = func(format string, a ...any) {
		if Log != nil {
			Log.Infof(format, a...)
		}
	}
	return &Daemon{
		dataDir: dataDir,
		store:   st,
		login:   zcode.NewLoginManager(st),
		stat:    stat.Open(dataDir),
	}
}

// Serve 运行插件（call 或 stdio 两种形态，共用同一份方法表）。
func Serve(ctx context.Context, mode, dataDir string, in io.Reader, out io.Writer) error {
	r := registry(NewDaemon(dataDir))
	switch mode {
	case plugin.ModeCall:
		return r.ServeOnce(ctx, in, out)
	default:
		return r.ServeStdio(ctx, in, out)
	}
}

// registry 组装完整方法表（控制面 + 会话面）。
func registry(d *Daemon) *plugin.Registry {
	r := plugin.New()
	r.Lifecycle(Manifest())

	r.Handle("config.schema", func(_ context.Context, _ Raw) (Raw, *Err) {
		return mustJSON(configSchema()), nil
	})
	r.Handle("config.get", func(_ context.Context, p Raw) (Raw, *Err) {
		cfg := d.loadConfig()
		if in := parseConfig(p); in.Region != "" || in.BaseURL != "" || in.Auth != "" {
			cfg = mergeOne(cfg, in)
		}
		return mustJSON(map[string]any{"config": cfg}), nil
	})
	r.Handle("config.set", func(_ context.Context, p Raw) (Raw, *Err) {
		in := parseConfig(p)
		cfg := mergeOne(d.loadConfig(), in)
		if err := d.saveConfig(cfg); err != nil {
			return nil, errf(errkind.BadParams, errkind.ScopeRequest, "保存配置失败: %v", err)
		}
		return mustJSON(map[string]any{"ok": true}), nil
	})

	r.Handle("auth.methods", func(_ context.Context, p Raw) (Raw, *Err) {
		return mustJSON(d.authMethods(p)), nil
	})
	r.Handle("auth.begin", func(ctx context.Context, p Raw) (Raw, *Err) {
		return d.authBegin(ctx, p)
	})
	r.Handle("auth.status", func(ctx context.Context, p Raw) (Raw, *Err) {
		return d.authStatus(ctx, p)
	})
	r.Handle("auth.finish", func(_ context.Context, p Raw) (Raw, *Err) {
		// device_poll 的授权在**服务端**完成（回调 /oauth/cli/callback/<provider>），
		// 不经本地端口、也不经 zcode:// 自定义协议 ⇒ 没有需要用户粘贴回来的东西。
		// 明确拒绝而不是静默成功：调用方应改用 auth.status 轮询。
		return nil, errf(errkind.BadParams, errkind.ScopeRequest,
			"device_poll 登录无需回调提交，请使用 auth.status 轮询登录结果")
	})
	r.Handle("auth.cancel", func(_ context.Context, p Raw) (Raw, *Err) {
		var req authflow.CancelRequest
		_ = json.Unmarshal(p, &req)
		if req.FlowID == "" {
			return nil, errf(errkind.BadParams, errkind.ScopeRequest, "缺少 flow_id")
		}
		d.login.Cancel(req.FlowID)
		return mustJSON(map[string]any{"ok": true}), nil
	})

	r.Handle("accounts.list", func(_ context.Context, _ Raw) (Raw, *Err) {
		return d.accountsList()
	})
	r.Handle("accounts.resource_detail", func(ctx context.Context, p Raw) (Raw, *Err) {
		return d.resourceDetail(ctx, p)
	})
	r.Handle("accounts.refresh", func(ctx context.Context, p Raw) (Raw, *Err) {
		return d.accountRefresh(ctx, p)
	})
	r.Handle("models.list", func(ctx context.Context, p Raw) (Raw, *Err) {
		return d.modelsList(ctx, p)
	})
	r.Handle("credits.get", func(ctx context.Context, p Raw) (Raw, *Err) {
		return d.creditsGet(ctx, p)
	})
	r.Handle("stats.get", func(_ context.Context, p Raw) (Raw, *Err) {
		return d.statsGet(p)
	})
	r.HandleStream("chat.generate", d.generate)
	return r
}

// ---------- 配置 ----------

func (d *Daemon) configPath() string {
	if d.dataDir == "" {
		return ""
	}
	return filepath.Join(d.dataDir, "config.json")
}

func (d *Daemon) loadConfig() zcode.Config {
	p := d.configPath()
	if p == "" {
		return zcode.Config{}
	}
	b, err := os.ReadFile(p)
	if err != nil {
		return zcode.Config{}
	}
	var cfg zcode.Config
	_ = json.Unmarshal(b, &cfg)
	return cfg
}

func (d *Daemon) saveConfig(cfg zcode.Config) error {
	p := d.configPath()
	if p == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		return err
	}
	b, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	tmp := p + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, p)
}

func mergeOne(base, in zcode.Config) zcode.Config {
	if in.Region != "" {
		base.Region = in.Region
	}
	if in.Proxy != "" {
		base.Proxy = in.Proxy
	}
	if in.Timeout != 0 {
		base.Timeout = in.Timeout
	}
	if in.BaseURL != "" {
		base.BaseURL = in.BaseURL
	}
	if in.Auth != "" {
		base.Auth = in.Auth
	}
	return base
}

// parseConfig 解析 core 下发的渠道配置（`{"config":{...}}` 或裸对象）。
func parseConfig(p Raw) zcode.Config {
	var w struct {
		Config zcode.Config `json:"config"`
	}
	if err := json.Unmarshal(p, &w); err == nil &&
		(w.Config.Region != "" || w.Config.BaseURL != "" || w.Config.Timeout != 0) {
		return w.Config
	}
	var direct zcode.Config
	_ = json.Unmarshal(p, &direct)
	return direct
}

// channelConfig 解析 chat.generate 参数里的 channel_config 字段。
func channelConfig(raw Raw) zcode.Config {
	var w struct {
		ChannelConfig json.RawMessage `json:"channel_config"`
	}
	_ = json.Unmarshal(raw, &w)
	if len(w.ChannelConfig) == 0 {
		return zcode.Config{}
	}
	return zcode.ParseConfig(w.ChannelConfig)
}

// mergeConfig 用请求内嵌的渠道配置覆盖外层配置（非空字段优先）。
//
// ⚠ AGENTS 陷阱 20：渠道配置**只在** chat.GenerateRequest.ChannelConfig 里，
// `params.config` 不含它。漏掉这一步会让「面板里改了 base_url / proxy / 指定账号」
// 在会话面上完全失效，而控制面看起来一切正常。
func mergeConfig(cfg zcode.Config, req chat.GenerateRequest) zcode.Config {
	in := zcode.ParseConfig(req.ChannelConfig)
	if in.Region != "" {
		cfg.Region = in.Region
	}
	if in.Proxy != "" {
		cfg.Proxy = in.Proxy
	}
	if in.Timeout != 0 {
		cfg.Timeout = in.Timeout
	}
	if in.BaseURL != "" {
		cfg.BaseURL = in.BaseURL
	}
	if in.Auth != "" {
		cfg.Auth = in.Auth
	}
	return cfg
}

// uidOf 解析 params 里的账号选择。
func uidOf(p Raw) string {
	var w struct {
		UID string `json:"uid"`
	}
	_ = json.Unmarshal(p, &w)
	return w.UID
}

func configSchema() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"region": map[string]any{
				"type":        "string",
				"title":       "区域",
				"description": "渠道区域（当前仅 zcode）",
				"default":     zcode.DefaultRegion,
			},
			"proxy": map[string]any{
				"type":        "string",
				"title":       "代理",
				"description": "HTTP(S) 代理，例如 http://127.0.0.1:7890",
			},
			"timeout": map[string]any{
				"type":        "integer",
				"title":       "超时（秒）",
				"default":     300,
				"description": "上游请求超时（秒）；流式读取另由空闲看门狗兜底",
			},
			"base_url": map[string]any{
				"type":        "string",
				"title":       "上游基址",
				"default":     zcode.Origin,
				"description": "覆盖上游基址（联调/自建反代用）；同时作用于积分制与订阅制两条腿",
			},
			"auth": map[string]any{
				"type":        "string",
				"title":       "指定凭据",
				"description": "指定使用某个凭据文件名或 uid（多账号时用）",
			},
		},
	}
}

// ---------- 登录 ----------

func (d *Daemon) authMethods(p Raw) authflow.MethodsResult {
	cfg := parseConfig(p)
	region := cfg.Region
	if region == "" {
		region = zcode.DefaultRegion
	}
	return authflow.MethodsResult{
		Region: region,
		Methods: []authflow.Method{
			{
				ID:    "device_poll",
				Type:  authflow.TypeDevicePoll,
				Label: "设备授权登录",
				Description: "在浏览器中打开授权链接并确认（可用手机号/微信等官方方式登录），" +
					"插件轮询服务端拿凭据；无需本地端口，可跨机完成。",
				Region:  region,
				Default: true,
			},
		},
		Note: "凭据不可续期（JWT 无 exp、上游无 refresh 端点）；失效时需重新登录。",
	}
}

func (d *Daemon) authBegin(ctx context.Context, p Raw) (Raw, *Err) {
	cfg := parseConfig(p)
	flowID, authURL, err := d.login.Begin(ctx, cfg)
	if err != nil {
		return nil, errf(errkind.Auth, errkind.ScopeAccount, "发起登录失败: %v", err)
	}
	return mustJSON(authflow.BeginResult{
		FlowID:     flowID,
		Type:       authflow.TypeDevicePoll,
		Method:     "device_poll",
		AuthURL:    authURL,
		State:      string(authflow.StateWaitingUser),
		Prompt:     "请在浏览器中打开授权链接并完成确认，然后回到面板等待登录成功",
		ExpiresAt:  time.Now().Add(15 * time.Minute).Format(time.RFC3339),
		NextPollMS: 2000,
	}), nil
}

// authStatus 总是返回 StatusResult（错误以 Error 字段承载，Go error 为 nil）。
func (d *Daemon) authStatus(ctx context.Context, p Raw) (Raw, *Err) {
	var req authflow.StatusRequest
	_ = json.Unmarshal(p, &req)
	if req.FlowID == "" {
		return mustJSON(authflow.StatusResult{
			State: authflow.StateFailed,
			Hint:  "缺少 flow_id",
			Error: &authflow.Error{Kind: string(errkind.BadParams), Message: "缺少 flow_id"},
		}), nil
	}
	cfg := parseConfig(p)
	state, a, hint, err := d.login.Status(ctx, req.FlowID, cfg)
	res := authflow.StatusResult{State: authflow.State(state), Hint: hint, NextPollMS: 2000}
	if authflow.State(state).Terminal() {
		res.NextPollMS = 0
	}
	if err != nil {
		res.Error = &authflow.Error{Kind: string(errkind.Auth), Message: err.Error()}
	}
	if state == string(authflow.StateSuccess) && a != nil {
		nick := a.Nickname
		if nick == "" {
			nick = a.UID
		}
		res.Account = &authflow.Account{
			UID:       a.UID,
			Nickname:  nick,
			ChannelID: PluginID,
			Region:    a.RegionOf(),
			ExpiresAt: a.ExpiresAtText(),
			LoginKind: string(authflow.TypeDevicePoll),
			AuthFile:  a.AuthFile,
		}
	}
	return mustJSON(res), nil
}

// ---------- 账号 ----------

func (d *Daemon) accountsList() (Raw, *Err) {
	list, _ := d.store.List()
	out := make([]meta.AccountInfo, 0, len(list))
	for _, a := range list {
		uid := a.UID
		if uid == "" {
			uid = a.AuthFile
		}
		status := "ok"
		if a.Expired() {
			status = "expired"
		}
		out = append(out, meta.AccountInfo{
			UID:       uid,
			Nickname:  a.Nickname,
			Status:    status,
			LoginKind: string(authflow.TypeDevicePoll),
			ExpiresAt: a.ExpiresAtText(),
			ChannelID: PluginID,
			Region:    a.RegionOf(),
			Prefixes:  []string{zcode.AuthPrefix},
			AuthFile:  a.AuthFile,
		})
	}
	return mustJSON(map[string]any{"accounts": out}), nil
}

func (d *Daemon) resourceDetail(ctx context.Context, p Raw) (Raw, *Err) {
	cfg := parseConfig(p)
	a, aerr := d.account(cfg, uidOf(p))
	if aerr != nil {
		return nil, errf(aerr.Kind, scopeOfKind(aerr.Kind), "%s", aerr.Msg)
	}
	_, items, err := d.client(cfg).UserResourceDetail(a)
	if err != nil {
		e := zcodeErr(err, errkind.Server)
		return nil, errf(e.Kind, scopeOfKind(e.Kind), "获取资源失败: %s", e.Msg)
	}
	if items == nil {
		items = []meta.ResourceItem{}
	}
	// 契约（W3-17 / docs/WW3_FRONTEND_API.md §3.4）：额度明细返回**裸数组**，
	// 前端直接 map 渲染；余额由 accounts.list / credits.get 承载。
	return mustJSON(items), nil
}

// accountRefresh 本渠道**没有续期端点**，故这里不做「换新 token」的假动作。
//
// 诚实的语义：重新校验凭据是否仍然可用（打一次余额接口），
//   - 可用 → ok + 说明「凭据不可续期，仍有效」；
//   - 401 / code 1002 → SessionDead，提示重新登录。
//
// 为什么不能直接返回「刷新成功」：那会让调度器以为续期有效，从而不再触发重登录，
// 直到所有请求 401 才暴露（旧版刻意把 RefreshToken 写成空实现也是同一理由）。
func (d *Daemon) accountRefresh(ctx context.Context, p Raw) (Raw, *Err) {
	cfg := parseConfig(p)
	a, aerr := d.pickAuth(cfg, uidOf(p))
	if aerr != nil {
		return nil, errf(aerr.Kind, scopeOfKind(aerr.Kind), "%s", aerr.Msg)
	}
	remaining, err := d.client(cfg).UserResource(a)
	if err != nil {
		e := zcodeErr(err, errkind.SessionDead)
		if e.Kind == errkind.SessionDead {
			return nil, errf(errkind.SessionDead, errkind.ScopeAccount,
				"凭据已失效（%s）；本渠道不可续期，请重新登录", e.Msg)
		}
		return nil, errf(e.Kind, scopeOfKind(e.Kind), "校验凭据失败: %s", e.Msg)
	}
	return mustJSON(map[string]any{
		"ok": true,
		"message": fmt.Sprintf("凭据不可续期（JWT 无 exp，上游无 refresh 端点），仍有效；"+
			"剩余额度 %d tokens", remaining),
	}), nil
}

// ---------- 模型 / 积分 / 统计 ----------

func (d *Daemon) modelsList(ctx context.Context, p Raw) (Raw, *Err) {
	cfg := parseConfig(p)
	a, aerr := d.account(cfg, "")
	if aerr != nil {
		return mustJSON(map[string]any{"models": prefixModels(zcode.StaticModels())}), nil
	}
	ms, err := d.client(cfg).FetchModels(a)
	if err != nil || len(ms) == 0 {
		ms = zcode.StaticModels()
	}
	return mustJSON(map[string]any{"models": prefixModels(acceptedModels(ms))}), nil
}

// acceptedModels 过滤出「chat.generate 也接受」的模型。
//
// 为什么要过滤：会话面按契约必须**本地拒绝未知模型**（errkind.ModelBlocked），
// 而判据是本渠道的静态兜底表（旧版实测：Start Plan 下 GLM-5-Turbo / GLM-5.2
// 返回空响应，故刻意不暴露）。上游目录接口可能下发这两者或新模型，若原样列出来，
// 用户会看到一个「列表里有、一发就 model_blocked」的模型 —— 这是最差的一类不一致。
// 宁可列表比上游目录短，也不要自相矛盾。
func acceptedModels(ms []meta.ModelInfo) []meta.ModelInfo {
	out := make([]meta.ModelInfo, 0, len(ms))
	for _, m := range ms {
		if zcode.KnownModel(m.ID) {
			out = append(out, m)
		}
	}
	if len(out) == 0 {
		// 目录里一条可接受的都没有（上游换了命名）：回退静态表，不返回空列表。
		return zcode.StaticModels()
	}
	return out
}

func prefixModels(ms []meta.ModelInfo) []meta.ModelInfo {
	out := make([]meta.ModelInfo, 0, len(ms))
	for _, m := range ms {
		m.ID = zcode.PrefixModel(m.ID)
		out = append(out, m)
	}
	return out
}

func (d *Daemon) creditsGet(ctx context.Context, p Raw) (Raw, *Err) {
	cfg := parseConfig(p)
	a, aerr := d.account(cfg, uidOf(p))
	if aerr != nil {
		return mustJSON(map[string]any{"balance": nil, "items": []meta.ResourceItem{}}), nil
	}
	balance, items, err := d.client(cfg).UserResourceDetail(a)
	if err != nil {
		return mustJSON(map[string]any{"balance": nil, "items": []meta.ResourceItem{}}), nil
	}
	if items == nil {
		items = []meta.ResourceItem{}
	}
	// unit 用 token 而不是 credits：本渠道的额度口径是**token remaining**
	// （协议 §1.2.1：把 token 当积分上报会污染 ledger / stats，旧版 2026-10-17 已修正）。
	return mustJSON(map[string]any{"balance": balance, "items": items, "unit": "token"}), nil
}

func (d *Daemon) statsGet(p Raw) (Raw, *Err) {
	var q struct {
		Days int `json:"days"`
	}
	_ = json.Unmarshal(p, &q)
	days := q.Days
	if days <= 0 {
		days = 7
	}
	if d.stat == nil {
		return mustJSON(stat.Summary{Days: []stat.Day{}}), nil
	}
	return mustJSON(stat.Summarize(d.stat.Query(days))), nil
}

// ---------- 账号解析与统计写入 ----------

// zcodeErr 把上游错误转成本包的 *zcode.Error（未知错误按 fallback 归类）。
func zcodeErr(err error, fallback errkind.Kind) *zcode.Error {
	var e *zcode.Error
	if errors.As(err, &e) {
		return e
	}
	// 传输层失败（DNS/连接被拒/TLS/代理不可达）与上游业务错误分开：
	// 归成 Conn 才不会触发换号/冷却（换账号也连不上，罚号纯属误伤）。
	if kindOfNetErr(err) != "" {
		return &zcode.Error{Kind: kindOfNetErr(err), Msg: err.Error()}
	}
	return &zcode.Error{Kind: fallback, Msg: err.Error()}
}

// kindOfNetErr 判定网络层错误（对齐 raccoon 的 kindOfNetErr）。
func kindOfNetErr(err error) errkind.Kind {
	if err == nil {
		return ""
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return errkind.Cancel
	}
	var ue *url.Error
	if errors.As(err, &ue) {
		if ue.Timeout() {
			return errkind.IdleTimeout
		}
		return errkind.Conn
	}
	var ne net.Error
	if errors.As(err, &ne) {
		if ne.Timeout() {
			return errkind.IdleTimeout
		}
		return errkind.Conn
	}
	return ""
}

// account 选取可用账号（本渠道不可续期，故不做续期动作）。
func (d *Daemon) account(cfg zcode.Config, uid string) (*zcode.Auth, *zcode.Error) {
	return d.pickAuth(cfg, uid)
}

// pickAuth 按 uid / cfg.Auth / 列表顺序选取凭据。
func (d *Daemon) pickAuth(cfg zcode.Config, uid string) (*zcode.Auth, *zcode.Error) {
	if uid == "" {
		uid = cfg.Auth
	}
	if uid != "" {
		if a, err := d.store.Load(uid); err == nil {
			return a, nil
		}
		if a, err := d.store.FindByUID(uid); err == nil {
			return a, nil
		}
		return nil, &zcode.Error{Kind: errkind.Auth, Msg: fmt.Sprintf("未找到账号 %s，请先登录", uid)}
	}
	list, _ := d.store.List()
	var pick *zcode.Auth
	for _, a := range list {
		if !a.Expired() {
			pick = a
			break
		}
	}
	if pick == nil && len(list) > 0 {
		// 全部「过期」也选第一个：本渠道的 ExpiresAt 是远期值，走到这里说明凭据被手工改过，
		// 真正的失效判据是上游 401 / code 1002（见 zcode.Classify）。
		pick = list[0]
	}
	if pick == nil {
		return nil, &zcode.Error{Kind: errkind.Auth, Msg: "该渠道尚未登录，请先在面板完成登录"}
	}
	return pick, nil
}

// client 构造按配置定制的上游客户端（proxy / timeout / base_url 生效）。
func (d *Daemon) client(cfg zcode.Config) *zcode.Client {
	return zcode.NewFromConfig(cfg)
}

// statRecord 是待写入统计的一条记录（会话面内部结构）。
type statRecord struct {
	Kind               string
	OK                 bool
	Model              string
	Account            string
	Region             string
	ErrKind            string
	PromptTokens       int
	CompletionTokens   int
	TotalTokens        int
	CachedTokens       int
	CachedReported     bool
	PromptReported     bool
	CompletionReported bool
	TotalReported      bool
	FirstTokenMs       int
	Time               time.Duration
}

// appendStat 把内部记录写入 stat.Store（fail-open；未上报 token 记 -1）。
func (d *Daemon) appendStat(rec statRecord, _ zcode.Config) {
	if d.stat == nil {
		return
	}
	p, cp, t := rec.PromptTokens, rec.CompletionTokens, rec.TotalTokens
	if p == 0 && cp == 0 && t == 0 {
		// 契约：上游未上报用量写 -1（0 会被统计当成「确实是 0 token」）。
		p, cp, t = -1, -1, -1
	}
	d.stat.Append(stat.Record{
		Kind:               rec.Kind,
		Channel:            PluginID,
		Account:            rec.Account,
		Region:             rec.Region,
		Model:              rec.Model,
		ErrKind:            rec.ErrKind,
		OK:                 rec.OK,
		PromptTokens:       p,
		CompletionTokens:   cp,
		TotalTokens:        t,
		CachedTokens:       rec.CachedTokens,
		CachedReported:     rec.CachedReported,
		PromptReported:     rec.PromptReported,
		CompletionReported: rec.CompletionReported,
		TotalReported:      rec.TotalReported,
		TimeMs:             int(rec.Time.Milliseconds()),
		FirstTokenMs:       rec.FirstTokenMs,
	})
}
