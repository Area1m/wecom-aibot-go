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

type APIClient struct {
	httpClient *http.Client
	logger     Logger
}

func newAPIClient(logger Logger, timeoutMS int) *APIClient {
	if timeoutMS <= 0 {
		timeoutMS = DefaultRequestTimeout
	}
	return &APIClient{
		httpClient: &http.Client{Timeout: time.Duration(timeoutMS) * time.Millisecond},
		logger:     logger,
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

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return DownloadedFile{}, fmt.Errorf("读取文件内容失败: %w", err)
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
			decoded, err := url.QueryUnescape(v[idx+2:])
			if err == nil {
				return decoded
			}
		}
	}
	if v, ok := params["filename"]; ok {
		decoded, err := url.QueryUnescape(v)
		if err == nil {
			return decoded
		}
		return v
	}
	return ""
}
