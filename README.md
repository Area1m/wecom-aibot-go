# wecom-aibot-go

[![CI](https://github.com/Area1m/wecom-aibot-go/actions/workflows/ci.yml/badge.svg)](https://github.com/Area1m/wecom-aibot-go/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/Area1m/wecom-aibot-go.svg)](https://pkg.go.dev/github.com/Area1m/wecom-aibot-go)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

企业微信智能机器人 Go SDK（Go 1.22+），基于 WebSocket 长连接通道，提供消息接收、流式回复、模板卡片、主动发送、文件下载与 AES 解密能力。

- 默认连接地址：`wss://openws.work.weixin.qq.com`
- 并发模型：`goroutine + channel`
- WebSocket 实现：`github.com/gorilla/websocket`
- 日志：可插拔 `Logger` 接口

## 功能特性

- WebSocket 长连接与自动认证（`bot_id + secret`）
- 心跳保活（`ping`）与断线自动重连
- 指数退避重连（最大 30s）
- 消息/事件自动分发（文本、图片、语音、文件、事件）
- 同 `req_id` 串行回复队列与 ACK 等待
- 流式回复与流式+卡片组合回复
- 模板卡片回复、欢迎语回复、卡片更新
- 主动发送 Markdown / 模板卡片
- 文件下载与 AES-256-CBC 解密

## 安装

```bash
go get github.com/Area1m/wecom-aibot-go
```

> 如果你在本地仓库运行示例，可直接在 `wecom-aibot-go` 目录执行 `go mod tidy && go run ./examples`。

## 快速开始

```go
package main

import (
  "context"
  "log"

  wecomaibot "github.com/Area1m/wecom-aibot-go"
)

func main() {
  bot, err := wecomaibot.NewClient(wecomaibot.Config{
    BotID:  "your-bot-id",
    Secret: "your-bot-secret",
  })
  if err != nil {
    log.Fatalf("创建客户端失败: %v", err)
  }

  bot.OnAuthenticated(func(ctx context.Context) {
    log.Println("认证成功")
  })

  bot.OnText(func(ctx context.Context, msg wecomaibot.TextMessage) {
    _, err := bot.ReplyStream(msg, "你好，我是 Go SDK 机器人")
    if err != nil {
      log.Printf("回复失败: %v", err)
    }
  })

  if err := bot.Connect(); err != nil {
    log.Fatalf("连接失败: %v", err)
  }

  select {}
}
```

完整 API 文档见 [pkg.go.dev](https://pkg.go.dev/github.com/Area1m/wecom-aibot-go)。

示例运行：

- 基础示例：`go run ./examples`
- 生产模板：`go run ./examples/production`

## 配置项

`Config` 字段：

- `BotID`：机器人 ID（必填）
- `Secret`：机器人 Secret（必填）
- `ReconnectIntervalMS`：重连基础间隔，默认 `1000`
- `MaxReconnectAttempts`：最大重连次数，默认 `10`，`-1` 表示无限
- `HeartbeatIntervalMS`：心跳间隔，默认 `30000`
- `RequestTimeoutMS`：HTTP 下载超时，默认 `10000`
- `MaxDownloadBytes`：单次文件下载上限（字节），默认 `104857600`（100 MiB）
- `WSURL`：WebSocket 地址，默认 `wss://openws.work.weixin.qq.com`
- `Logger`：自定义日志器，默认 `DefaultLogger`

> 心跳说明：WeCom 服务端收到客户端 `ping` 帧后不做任何响应（既不回 `pong`，也不回 ACK，实测），
> 所以心跳只用于保活，**不会**因为「没收到心跳 ACK」而断开连接；连接失效由发送失败（写不出去）
> 或服务端被动断开来发现，两者都会触发正常重连。

## 事件注册

连接生命周期：

- `OnConnected`
- `OnAuthenticated`
- `OnDisconnected`
- `OnReconnecting`
- `OnError`

消息与事件：

- `OnMessage`
- `OnText`
- `OnImage`
- `OnMixed`
- `OnVoice`
- `OnFile`
- `OnEvent`
- `OnEnterChat`
- `OnTemplateCardEvent`
- `OnFeedbackEvent`

## 回复与发送 API

- `Reply(req, body, cmd)`：通用回复
- `ReplyStream(req, content)`：快速流式回复（单条结束）
- `ReplyStreamByID(req, streamID, content, finish, msgItem, feedback)`：完整流式控制
- `ReplyWelcomeText(req, content)` / `ReplyWelcomeTemplateCard(req, card)`
- `ReplyTemplateCard(req, card, feedback)`
- `ReplyStreamWithCard(req, streamID, content, finish, opts)`
- `UpdateTemplateCard(req, card, userIDs)`
- `SendMarkdown(chatID, content)`
- `SendTemplateCard(chatID, card)`

## 文件下载与解密

```go
bot.OnImage(func(ctx context.Context, msg wecomaibot.ImageMessage) {
  file, err := bot.DownloadFile(msg.Image.URL, msg.Image.AESKey)
  if err != nil {
    return
  }
  _ = file.Buffer
  _ = file.Filename
})
```

说明：

- 当 `aesKey` 为空时，返回原始下载数据
- 有 `aesKey` 时，执行 AES-256-CBC 解密（IV 取 key 前 16 字节）并手动去除 PKCS#7 填充

## 协议命令对照

- 认证：`aibot_subscribe`
- 心跳：`ping`
- 心跳应答：`pong`（仅在服务端主动 ping 时回包，当前服务端不发）
- 回复：`aibot_respond_msg`
- 欢迎语回复：`aibot_respond_welcome_msg`
- 更新卡片：`aibot_respond_update_msg`
- 主动发送：`aibot_send_msg`
- 消息回调：`aibot_msg_callback`
- 事件回调：`aibot_event_callback`

## 目录结构

```text
wecom-aibot-go/
  doc.go            包文档与用法示例
  client.go         客户端：连接、连接生命周期回调注册
  websocket.go      WebSocket 会话、帧收发与 ACK 关联
  auth.go           认证
  heartbeat.go      心跳保活
  reconnect.go      指数退避重连
  dispatcher.go     消息/事件分发
  reply.go          回复、流式回复与主动发送
  api.go            HTTP 侧能力（文件下载）
  crypto.go         AES-256-CBC 解密
  logger.go         日志接口与默认实现
  types.go          协议类型、常量与配置
  utils.go          req_id 生成
  examples/main.go
  examples/production/main.go
  .github/workflows/ci.yml
  LICENSE
```

## 开发与测试

```bash
gofmt -l .          # 检查格式
go vet ./...        # 静态检查
go test ./...       # 单元测试（含基于假 WebSocket 服务端的端到端用例）
go test -race ./... # 竞态检测（需要 cgo）
```

CI 在 GitHub Actions 上跑 `gofmt` / `go vet` / `go build` / `go test -race`，覆盖 Go 1.22 及以上。

## 开源协议

[MIT](LICENSE)

## 生产模板说明

`examples/production/main.go` 内置了常见生产能力：

- 生命周期日志（连接、认证、断开、重连、错误）
- 处理回调的 panic 恢复
- 回复失败重试（固定次数 + 退避间隔）
- 回复失败时降级发送提示消息
- 健康检查端点 `GET /healthz`（默认 `:8080`）

## 设计映射（Node -> Go）

- `WSClient` -> `Client`
- `EventEmitter` -> `OnXxx` handler 注册机制
- `WsConnectionManager` -> `wsConnection`
- `MessageHandler` -> `dispatcher`
- `WeComApiClient` -> `APIClient`
