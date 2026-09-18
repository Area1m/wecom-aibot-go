//go:build linux

package wecomaibot

import (
	"syscall"
	"time"
)

// TCP_USER_TIMEOUT 在 Linux <linux/tcp.h> 中值为 18，自内核 2.6.37（2011）起稳定不变。
const tcpUserTimeoutOpt = 0x12

func setsockoptTCPUserTimeout(fd int, d time.Duration) error {
	return syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, tcpUserTimeoutOpt, int(d/time.Millisecond))
}
