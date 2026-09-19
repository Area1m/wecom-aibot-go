// Package wecomaibot 是企业微信智能机器人的 Go SDK：基于 openws WebSocket 长连接
// 接收消息与事件，提供流式回复、模板卡片、欢迎语、主动发送、文件下载与 AES 解密，
// 内置认证、心跳保活与指数退避重连。
//
// 最小用法：
//
//	bot, err := wecomaibot.NewClient(wecomaibot.Config{
//		BotID:  "your-bot-id",
//		Secret: "your-bot-secret",
//	})
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	bot.OnText(func(ctx context.Context, msg wecomaibot.TextMessage) {
//		if _, err := bot.ReplyStreamByID(msg, wecomaibot.GenerateReqID("stream"), "你好，我是 Go SDK 机器人", true, nil, nil); err != nil {
//			log.Printf("回复失败: %v", err)
//		}
//	})
//
//	if err := bot.Connect(); err != nil { // 异步建连，认证成功触发 OnAuthenticated
//		log.Fatal(err)
//	}
//	defer bot.Disconnect()
//
// 连接生命周期由 OnConnected / OnAuthenticated / OnDisconnected / OnReconnecting /
// OnError 通知；发送失败时可用 errors.Is(err, ErrNotConnected) 判断是否只是连接暂时
// 不可用（等重连后重试即可）。
//
// 所有 OnXxx 回调都在独立 goroutine 中执行，因此某个回调阻塞不会影响收包；但也意味着
// 回调里未捕获的 panic 会终止整个进程（Go 的 goroutine 语义），需要调用方自行 recover
// ——可参考 examples/production 里的 safeGo。
//
// 心跳保活：服务端会回 ping 的 ACK（errcode:0），SDK 以「连续 2 次未收到 ACK」判定死连接并
// 重连（对齐官方 Node SDK）；写失败或服务端被动断开同样会触发重连，半开连接由 TCP_USER_TIMEOUT 兜底。
//
// 重连预算分两本账（对齐官方 Node SDK）：认证失败走 MaxAuthFailureAttempts（默认 5），连接断开走
// MaxReconnectAttempts（默认 10），互不占用——认证抖动不会吞掉断线重连预算。
package wecomaibot
