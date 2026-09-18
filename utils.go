package wecomaibot

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"
)

// GenerateRandomString 生成长度为 length 的随机十六进制字符串（对应官方 generate_random_string）。
// length 小于等于 0 时回落到默认 8。
func GenerateRandomString(length int) string {
	if length <= 0 {
		length = 8
	}
	byteLen := (length + 1) / 2
	b := make([]byte, byteLen)
	if _, err := rand.Read(b); err != nil {
		// 随机源不可用（几乎不会发生）时兜底：用纳秒时间戳反复拼接，保证长度足够，
		// 不能直接切 [:length]——时间戳 hex 只有 16 位，length 更大时会越界 panic。
		seed := fmt.Sprintf("%x", time.Now().UnixNano())
		out := make([]byte, 0, length)
		for len(out) < length {
			out = append(out, seed...)
		}
		return string(out[:length])
	}
	s := hex.EncodeToString(b)
	if len(s) > length {
		return s[:length]
	}
	return s
}

// GenerateReqID 生成请求 ID：<prefix>_<毫秒时间戳>_<8 位随机 hex>，用于认证、心跳、
// 主动发送与流式回复的 streamID 等需要全局唯一标识的场景。
func GenerateReqID(prefix string) string {
	return fmt.Sprintf("%s_%d_%s", prefix, time.Now().UnixMilli(), GenerateRandomString(8))
}
