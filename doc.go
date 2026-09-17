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
//		if _, err := bot.ReplyStream(msg, "你好，我是 Go SDK 机器人"); err != nil {
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
// 心跳只做保活：WeCom 服务端收到 ping 后不回 ACK，所以 SDK 不会因为「心跳没被应答」而
// 断开连接；连接失效由发送失败或服务端被动断开发现，随后自动重连。
package wecomaibot
