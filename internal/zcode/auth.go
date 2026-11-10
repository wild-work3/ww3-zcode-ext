package zcode

import (
	"encoding/json"
	"strings"
	"time"
)

// Config 是 channel_config 里与 zcode 相关的配置项。
type Config struct {
	Region  string `json:"region,omitempty"`
	Proxy   string `json:"proxy,omitempty"`
	Timeout int    `json:"timeout,omitempty"`  // 秒；0 表示默认
	BaseURL string `json:"base_url,omitempty"` // 覆盖平台 origin（联调用）
	Auth    string `json:"auth,omitempty"`     // 指定凭据名（可选）
}

// ParseConfig 尽力解析渠道配置；空/非法时返回零值（不报错，交由调用方决定）。
func ParseConfig(raw json.RawMessage) Config {
	var c Config
	if len(raw) == 0 {
		return c
	}
	_ = json.Unmarshal(raw, &c)
	return c
}

// SiteURL 返回平台 origin（含协议）。
func (c Config) SiteURL() string {
	if s := strings.TrimSpace(c.BaseURL); s != "" {
		return strings.TrimRight(s, "/")
	}
	return Origin
}

// TimeoutDur 返回业务请求超时；<=0 用默认值。
func (c Config) TimeoutDur() time.Duration {
	if c.Timeout > 0 {
		return time.Duration(c.Timeout) * time.Second
	}
	return requestTimeout
}

// Auth 是落盘的凭据体（内部私有，绝不外泄 token）。
//
// 与旧版 internal/auth.Auth 的差异：
//   - 去掉 Kind / ApiHost / MachineID 等本渠道不用的字段；
//   - 保留 Extra（上游 app_version / device_mid / coding_plan_key_* 都在这里）；
//   - 不加 mutex：插件每进程只读凭据，写入只发生在登录流程（单进程顺序）。
type Auth struct {
	AccessToken  string            `json:"accessToken"`
	RefreshToken string            `json:"refreshToken,omitempty"`
	ExpiresAt    int64             `json:"expiresAt,omitempty"` // Unix 秒
	Domain       string            `json:"domain,omitempty"`
	UID          string            `json:"uid,omitempty"`
	Nickname     string            `json:"nickname,omitempty"`
	Region       string            `json:"region,omitempty"`
	Extra        map[string]string `json:"extra,omitempty"`
	AuthFile     string            `json:"-"`
}

// AccessTokenValue 返回可用的访问令牌（旧版 auth.Auth 同名方法的对等物）。
func (a *Auth) AccessTokenValue() string {
	if a == nil {
		return ""
	}
	return strings.TrimSpace(a.AccessToken)
}

// RefreshTokenValue 返回续期令牌（本渠道恒为空，见包注释「凭据不可续期」）。
func (a *Auth) RefreshTokenValue() string {
	if a == nil {
		return ""
	}
	return strings.TrimSpace(a.RefreshToken)
}

// ExpiresAtText 返回 RFC3339 文本（空表示未知）。
func (a *Auth) ExpiresAtText() string {
	if a == nil || a.ExpiresAt <= 0 {
		return ""
	}
	return time.Unix(a.ExpiresAt, 0).Format(time.RFC3339)
}

// RegionOf 返回凭据所属区域（本渠道单区域）。
func (a *Auth) RegionOf() string {
	if a == nil || strings.TrimSpace(a.Region) == "" {
		return AuthPrefix
	}
	return a.Region
}

// Expired 判断凭据是否已过期（预留 60s 余量）。
//
// ⚠ 本渠道**不可续期**，所以这里的判定只用于「显示成 expired」，不驱动刷新：
// 登录时把 ExpiresAt 写成远期值（farFutureExpiry），正常情况下永不为真。
func (a *Auth) Expired() bool {
	if a == nil || a.ExpiresAt <= 0 {
		return false
	}
	return time.Now().Unix() >= a.ExpiresAt-60
}

// ExtraOf 安全读取 Extra。
func (a *Auth) ExtraOf(key string) string {
	if a == nil || a.Extra == nil {
		return ""
	}
	return strings.TrimSpace(a.Extra[key])
}
