package wecomaibot

import (
	"context"
	"net"
	"time"
)

// tcpUserTimeout 是半开连接的检测时限：连接建立后，一旦存在「已发送但超过该时长仍未
// 收到 ACK」的数据，内核就强制关闭连接，使阻塞的读循环立即返回错误并触发重连。
//
// 为什么必须用它：WeCom 空闲时既不回 ping、也不主动发任何数据，所以「读超时」和
// 「心跳 ACK 超时」都会误杀健康但安静的空闲连接（上次提交特意去掉的自掐逻辑）；而
// 客户端每 HeartbeatIntervalMS 发一次 ping，又让 TCP keepalive 永远等不到「空闲」。
// 只有 TCP_USER_TIMEOUT 基于「发出的数据有没有被 ACK」，能精确区分「对端已死」与
// 「对端健康但安静」。
const tcpUserTimeout = 60 * time.Second

// dial 建立底层 TCP 连接并设置 TCP_USER_TIMEOUT（仅 Linux 生效，其它平台静默跳过）。
// 作为 websocket.Dialer 的 NetDialContext 使用，从根上解决「日志静默、healthz 仍 OK、
// 需重启恢复」的半开连接假死。
func (w *wsConnection) dial(ctx context.Context, network, addr string) (net.Conn, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, network, addr)
	if err != nil {
		return nil, err
	}
	// 设置失败不视为致命错误：连接仍可用，只是少了半开连接的快速检测。
	if err := setTCPUserTimeout(conn, tcpUserTimeout); err != nil {
		w.logger.Warn("设置 TCP_USER_TIMEOUT 失败（不影响连接，仅半开检测失效）: %v", err)
	}
	return conn, nil
}

func setTCPUserTimeout(conn net.Conn, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	tc, ok := conn.(*net.TCPConn)
	if !ok {
		return nil // 非 TCP 连接（本 SDK 只会拨 TCP，此处仅防御）
	}
	raw, err := tc.SyscallConn()
	if err != nil {
		return err
	}
	var sockErr error
	if err := raw.Control(func(fd uintptr) {
		sockErr = setsockoptTCPUserTimeout(int(fd), d)
	}); err != nil {
		return err
	}
	return sockErr
}
