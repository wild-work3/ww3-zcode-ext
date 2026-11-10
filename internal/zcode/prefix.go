package zcode

import "strings"

// AuthPrefix 凭据文件名前缀（`<data>/auths/zcode-<uid>.json`）。
const AuthPrefix = kindName

// ChannelName 渠道名（stats / 日志用）。
const ChannelName = kindName

// DefaultRegion 单区域渠道的默认 region 值。
const DefaultRegion = kindName

// DefaultModel 无凭据时的默认模型（静态表首条）。
const DefaultModel = "GLM-5.3-Flash"

// PrefixModel 给模型 ID 加渠道前缀（幂等）。
func PrefixModel(id string) string {
	id = strings.TrimSpace(id)
	if id == "" {
		return ""
	}
	if strings.HasPrefix(id, kindPrefix) {
		return id
	}
	return kindPrefix + id
}

// TrimModel 去掉渠道前缀（不认识的模型原样返回）。
func TrimModel(id string) string {
	id = strings.TrimSpace(id)
	return strings.TrimPrefix(id, kindPrefix)
}

// KnownModel 报告模型是否在本渠道的静态兜底表里（**区分大小写**，
// 因为上游 model id 是大小写敏感的）。
func KnownModel(id string) bool {
	_, ok := staticIndex[TrimModel(id)]
	return ok
}
