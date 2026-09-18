package wecomaibot

import (
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// APIClient 承载 HTTP 侧能力（文件下载等非 WebSocket 请求），由 Client.API 暴露。
type APIClient struct {
	httpClient       *http.Client
	logger           Logger
	maxDownloadBytes int64
}

func newAPIClient(logger Logger, timeoutMS int, maxDownloadBytes int) *APIClient {
	if timeoutMS <= 0 {
		timeoutMS = DefaultRequestTimeout
	}
	if maxDownloadBytes <= 0 {
		maxDownloadBytes = DefaultMaxDownloadBytes
	}
	return &APIClient{
		httpClient:       &http.Client{Timeout: time.Duration(timeoutMS) * time.Millisecond},
		logger:           logger,
		maxDownloadBytes: int64(maxDownloadBytes),
	}
}

// DownloadRaw 下载文件并返回原始字节与文件名（不参与解密）。文件名取自响应头
// Content-Disposition。这是 Client.API() 暴露的底层能力，对应官方 SDK 的 download_file_raw。
func (c *APIClient) DownloadRaw(urlStr string) (DownloadedFile, error) {
	c.logger.Info("开始下载文件")
	req, err := http.NewRequest(http.MethodGet, urlStr, nil)
	if err != nil {
		return DownloadedFile{}, fmt.Errorf("构建下载请求失败: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return DownloadedFile{}, fmt.Errorf("下载文件失败: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return DownloadedFile{}, fmt.Errorf("下载文件失败: status=%d", resp.StatusCode)
	}

	if resp.ContentLength > c.maxDownloadBytes {
		return DownloadedFile{}, fmt.Errorf("下载文件失败: 文件过大 %d 字节，上限 %d 字节", resp.ContentLength, c.maxDownloadBytes)
	}

	// 多读 1 字节用来判断是否超限（Content-Length 可能缺失或不可信）。
	data, err := io.ReadAll(io.LimitReader(resp.Body, c.maxDownloadBytes+1))
	if err != nil {
		return DownloadedFile{}, fmt.Errorf("读取文件内容失败: %w", err)
	}
	if int64(len(data)) > c.maxDownloadBytes {
		return DownloadedFile{}, fmt.Errorf("下载文件失败: 文件超过上限 %d 字节", c.maxDownloadBytes)
	}

	c.logger.Info("文件下载成功")
	return DownloadedFile{
		Buffer:   data,
		Filename: parseFilename(resp.Header.Get("Content-Disposition")),
	}, nil
}

func parseFilename(contentDisposition string) string {
	if strings.TrimSpace(contentDisposition) == "" {
		return ""
	}
	_, params, err := mime.ParseMediaType(contentDisposition)
	if err != nil {
		return ""
	}

	v, ok := params["filename"]
	if !ok {
		return ""
	}
	// Go 的 mime.ParseMediaType 已把 RFC 5987 的 filename*=UTF-8''... 解码并折叠进
	// filename 参数（params 里不会保留 filename* 键），续传形式 filename*0*= 同样会折叠。
	// 此时值已被解码一次，若再 PathUnescape 一次就会双重解码，损坏文件名里字面的 %xx。
	// 用 ToLower 后 Contains "filename*" 判断（不拼 '='），以同时覆盖 filename*= 与
	// filename*0*= 续传形式。
	if strings.Contains(strings.ToLower(contentDisposition), "filename*") {
		return v
	}
	// 普通 filename 未做百分号解码，这里解码一次即与官方 urllib.unquote 对齐。
	// 用 PathUnescape 而非 QueryUnescape，避免把合法的 '+' 解成空格。
	decoded, err := url.PathUnescape(v)
	if err == nil {
		return decoded
	}
	return v
}
