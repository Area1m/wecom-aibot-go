package wecomaibot

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestParseFilename(t *testing.T) {
	cases := []struct {
		header string
		want   string
	}{
		{"", ""},
		{`attachment; filename="report.pdf"`, "report.pdf"},
		// 用 PathUnescape：RFC 5987 的百分号编码里 '+' 是合法字符，不能解成空格
		{`attachment; filename="a+b.txt"`, "a+b.txt"},
		{`attachment; filename="报告 v2.pdf"`, "报告 v2.pdf"},
		{`attachment; filename*=UTF-8''%E6%8A%A5%E5%91%8A.pdf`, "报告.pdf"},
		{`attachment`, ""},
	}
	for _, c := range cases {
		if got := parseFilename(c.header); got != c.want {
			t.Errorf("parseFilename(%q) = %q, want %q", c.header, got, c.want)
		}
	}
}

// DownloadFile 的 URL 来自消息内容，必须对超大响应设上限，否则一个异常响应就能打爆内存。
func TestDownloadFileRejectsOversizedBody(t *testing.T) {
	const cap = 64

	t.Run("声明了 Content-Length", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Length", "104857600")
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Close()

		api := newAPIClient(silentTestLogger{}, 5000, cap)
		if _, err := api.downloadFileRaw(srv.URL); err == nil || !strings.Contains(err.Error(), "文件过大") {
			t.Errorf("应按 Content-Length 拒绝超大文件: got %v", err)
		}
	})

	t.Run("分块返回（无 Content-Length）", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// 不设 Content-Length，直接分块写超过上限的数据
			for i := 0; i < 10; i++ {
				_, _ = w.Write(make([]byte, cap/2))
				w.(http.Flusher).Flush()
			}
		}))
		defer srv.Close()

		api := newAPIClient(silentTestLogger{}, 5000, cap)
		if _, err := api.downloadFileRaw(srv.URL); err == nil || !strings.Contains(err.Error(), "超过上限") {
			t.Errorf("应拒绝超限的分块响应: got %v", err)
		}
	})

	t.Run("正常大小", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Disposition", `attachment; filename="ok.txt"`)
			_, _ = w.Write([]byte("hello"))
		}))
		defer srv.Close()

		api := newAPIClient(silentTestLogger{}, 5000, cap)
		got, err := api.downloadFileRaw(srv.URL)
		if err != nil {
			t.Fatalf("下载失败: %v", err)
		}
		if string(got.Buffer) != "hello" || got.Filename != "ok.txt" {
			t.Errorf("下载结果错误: %+v", got)
		}
	})
}
