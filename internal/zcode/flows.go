// flows.go device_poll 登录的**插件侧流程管理**：把旧版 login_zcode 的
// Start / Poll / SaveAuth（三个独立入口、状态落 login-state.json）接到
// ww3 的 auth.begin / auth.status / auth.cancel 三个方法上。
//
// W3-20（关键约束）：渠道插件**无常驻进程**，core 每次调用都是一个新进程
// （`zcode-ext.exe call`）。auth.begin 与随后的 auth.status 必然落在不同进程里，
// 因此流程状态必须落盘：
//
//	<data>/auths/flows/<flow_id>.json        本文件的 flowMeta（region/proxy/base_url/创建时刻）
//	<data>/auths/flows/<flow_id>.state.json  旧版 login_zcode 的 state（flowId/flowSecret/…）
//
// 两个文件分开而不是合并：.state.json 的字节形态由 login.go 的 readState/writeState
// 独占（含 flowSecret，0600），本文件只追加「这次流程该怎么连上游」的元数据，
// 两者互不覆盖。旧版把流程放在 daemon 内存 map，撤 daemon 后跨进程查不到。
package zcode

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// flowTimeout 单次登录流程的最长存活时间。
//
// 上游自己也会给 expires_at（缺省时 Start 保守取 5 分钟），这里只是本地兜底：
// 防止用户点了登录就再也没回来，flows 目录里留下永远 waiting 的僵尸流程。
const flowTimeout = 15 * time.Minute

// flowMeta 是一次在途登录流程的本地元数据。
type flowMeta struct {
	ID        string    `json:"id"`
	Region    string    `json:"region,omitempty"`
	Proxy     string    `json:"proxy,omitempty"`
	BaseURL   string    `json:"base_url,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	LastErr   string    `json:"last_err,omitempty"`
}

// LoginManager 管理在途登录流程（落盘，无常驻）。
type LoginManager struct {
	dir   string // <data>/auths/flows
	store *Store
}

// NewLoginManager 创建登录流程管理器。
func NewLoginManager(store *Store) *LoginManager {
	return &LoginManager{dir: filepath.Join(store.Dir, "flows"), store: store}
}

func (m *LoginManager) metaPath(id string) string  { return filepath.Join(m.dir, id+".json") }
func (m *LoginManager) statePath(id string) string { return filepath.Join(m.dir, id+".state.json") }

// saveMeta 原子写流程元数据（0600）。
func (m *LoginManager) saveMeta(f *flowMeta) error {
	if err := os.MkdirAll(m.dir, 0o700); err != nil {
		return err
	}
	b, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return err
	}
	dst := m.metaPath(f.ID)
	tmp := dst + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, dst)
}

func (m *LoginManager) loadMeta(id string) (*flowMeta, bool) {
	if id == "" || strings.ContainsAny(id, `/\`) {
		return nil, false
	}
	b, err := os.ReadFile(m.metaPath(id))
	if err != nil {
		return nil, false
	}
	var f flowMeta
	if err := json.Unmarshal(b, &f); err != nil || f.ID == "" {
		return nil, false
	}
	return &f, true
}

// dropFlow 删除流程元数据与旧版 state（幂等）。
func (m *LoginManager) dropFlow(id string) {
	_ = os.Remove(m.metaPath(id))
	_ = os.Remove(m.statePath(id))
}

// gcFlows 清理超时僵尸流程（begin 时顺手调用，避免目录无限增长）。
func (m *LoginManager) gcFlows() {
	ents, err := os.ReadDir(m.dir)
	if err != nil {
		return
	}
	for _, e := range ents {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") || strings.HasSuffix(e.Name(), ".state.json") {
			continue
		}
		id := strings.TrimSuffix(e.Name(), ".json")
		if f, ok := m.loadMeta(id); ok && time.Since(f.CreatedAt) > flowTimeout {
			m.dropFlow(id)
		}
	}
}

// Begin 发起设备授权流：POST /api/v1/oauth/cli/init，拿到 authorize_url 后**立即返回**。
//
// 只发起、不轮询：前端 window.open 只在用户点击的瞬态激活窗口内有效，
// 轮询由 core 按 NextPollMS 驱动 auth.status（每次一个独立进程）。
func (m *LoginManager) Begin(ctx context.Context, cfg Config) (flowID, authURL string, err error) {
	m.gcFlows()
	id := "zcode-" + randomHex(8)
	url, err := StartAt(loginHTTPClient(cfg), m.statePath(id), cfg.SiteURL())
	if err != nil {
		return "", "", err
	}
	meta := &flowMeta{ID: id, Region: cfg.Region, Proxy: cfg.Proxy, BaseURL: cfg.BaseURL, CreatedAt: time.Now()}
	if err := m.saveMeta(meta); err != nil {
		return "", "", err
	}
	return id, url, nil
}

// Status 轮询一次登录状态（幂等，可反复调用；每次都是一次独立进程）。
// 返回 state ∈ waiting_user / success / failed / expired。
func (m *LoginManager) Status(ctx context.Context, flowID string, cfg Config) (state string, account *Auth, hint string, err error) {
	meta, ok := m.loadMeta(flowID)
	if !ok {
		return "expired", nil, "登录流程不存在或已过期，请重新发起", nil
	}
	if time.Since(meta.CreatedAt) > flowTimeout {
		m.dropFlow(flowID)
		return "expired", nil, "登录超时（15 分钟），请重新发起", nil
	}
	// 发起时的连接参数优先：status 可能由另一个进程、另一份 config 调起。
	if cfg.BaseURL == "" {
		cfg.BaseURL = meta.BaseURL
	}
	if cfg.Proxy == "" {
		cfg.Proxy = meta.Proxy
	}

	res, perr := PollAt(loginHTTPClient(cfg), m.statePath(flowID), cfg.SiteURL())
	if perr != nil {
		if errors.Is(perr, ErrPending) {
			return "waiting_user", nil, "请在浏览器中完成授权", nil
		}
		if isExpiredErr(perr) {
			m.dropFlow(flowID)
			return "expired", nil, perr.Error(), nil
		}
		// 失败也保留流程（下一次轮询可能恢复），只记录原因。
		meta.LastErr = perr.Error()
		_ = m.saveMeta(meta)
		return "failed", nil, perr.Error(), nil
	}

	path, serr := SaveAuth(m.store, res)
	if serr != nil {
		return "failed", nil, "", fmt.Errorf("保存凭据失败: %w", serr)
	}
	m.dropFlow(flowID)

	name := strings.TrimSuffix(filepath.Base(path), ".json")
	a, lerr := m.store.Load(name)
	if lerr != nil {
		// 凭据已落盘，读回失败不影响登录结论：回一个最小事实。
		a = &Auth{UID: res.UID, Nickname: res.Nickname, ExpiresAt: res.ExpiresAt, AuthFile: name}
	}
	return "success", a, "登录成功", nil
}

// Cancel 取消在途流程（删除流程文件；不存在也算取消成功，保持幂等）。
func (m *LoginManager) Cancel(flowID string) bool {
	m.dropFlow(flowID)
	return true
}

// loginHTTPClient 登录专用客户端（30s 总超时；代理来自渠道配置）。
func loginHTTPClient(cfg Config) *http.Client {
	return &http.Client{Timeout: 30 * time.Second, Transport: newTransport(cfg.Proxy)}
}

// isExpiredErr 判定「流程已过期」类终态错误（与 failed 区分：前端提示不同）。
func isExpiredErr(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "已过期") || strings.Contains(s, "过期")
}
