//go:build !linux

package wecomaibot

import "time"

// 非 Linux 平台没有 TCP_USER_TIMEOUT，静默跳过：半开连接检测退回到「写失败 / 服务端
// 主动断开」两条慢路径兜底。
func setsockoptTCPUserTimeout(fd int, d time.Duration) error {
	return nil
}
