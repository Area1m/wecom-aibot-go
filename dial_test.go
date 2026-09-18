//go:build linux

package wecomaibot

import (
	"net"
	"syscall"
	"testing"
	"time"
)

// 验证 TCP_USER_TIMEOUT 选项能成功写入并在内核侧读回相同值——既覆盖「设置不报错」，
// 也锁住选项常量与单位换算（毫秒）不回归。
func TestSetTCPUserTimeout(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("监听失败: %v", err)
	}
	defer ln.Close()

	go func() {
		c, err := ln.Accept()
		if err == nil {
			_ = c.Close()
		}
	}()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("拨号失败: %v", err)
	}
	defer conn.Close()

	if err := setTCPUserTimeout(conn, tcpUserTimeout); err != nil {
		t.Fatalf("设置 TCP_USER_TIMEOUT 失败: %v", err)
	}

	tc, ok := conn.(*net.TCPConn)
	if !ok {
		t.Fatal("连接应为 *net.TCPConn")
	}
	raw, err := tc.SyscallConn()
	if err != nil {
		t.Fatalf("获取 SyscallConn 失败: %v", err)
	}

	var got int
	if err := raw.Control(func(fd uintptr) {
		got, err = syscall.GetsockoptInt(int(fd), syscall.IPPROTO_TCP, tcpUserTimeoutOpt)
	}); err != nil {
		t.Fatalf("读回 TCP_USER_TIMEOUT 失败: %v", err)
	}
	want := int(tcpUserTimeout / time.Millisecond)
	if got != want {
		t.Errorf("TCP_USER_TIMEOUT 读回 %dms, 期望 %dms", got, want)
	}
}
