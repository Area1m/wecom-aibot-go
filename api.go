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

func (c *APIClient) downloadFileRaw(urlStr string) (DownloadedFile, error) {
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

	if v, ok := params["filename*"]; ok {
		if idx := strings.Index(v, "''"); idx >= 0 {
			// RFC 5987 的百分号编码，用 PathUnescape——QueryUnescape 会把文件名里
			// 合法的 '+' 解成空格。
			decoded, err := url.PathUnescape(v[idx+2:])
			if err == nil {
				return decoded
			}
		}
	}
	if v, ok := params["filename"]; ok {
		decoded, err := url.PathUnescape(v)
		if err == nil {
			return decoded
		}
		return v
	}
	return ""
}
