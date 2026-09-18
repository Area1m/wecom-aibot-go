package wecomaibot

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
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
		// filename* 已被 ParseMediaType 折叠并解码一次，不能二次解码损坏字面 %xx
		{`attachment; filename*=UTF-8''abc%2520def.txt`, "abc%20def.txt"},
		// 续传形式 filename*0* 与大小写不敏感都应单次解码
		{`attachment; filename*0*=UTF-8''abc%2520; filename*1*=def.txt`, "abc%20def.txt"},
		{`attachment; FILENAME*=UTF-8''abc%2520def.txt`, "abc%20def.txt"},
		// 带语言标签
		{`attachment; filename*=UTF-8'en'%E6%8A%A5.pdf`, "报.pdf"},
		// 无 charset 的 filename* 被标准库丢弃
		{`attachment; filename*=report.pdf`, ""},
		// ParseMediaType 报错
		{`attachment; filename="unterminated`, ""},
		// 非法百分号编码回退原值
		{`attachment; filename="a%zzb.txt"`, "a%zzb.txt"},
		// 普通 filename 只解码一次
		{`attachment; filename="a%20b.txt"`, "a b.txt"},
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
		if _, err := api.DownloadRaw(srv.URL); err == nil || !strings.Contains(err.Error(), "文件过大") {
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
		if _, err := api.DownloadRaw(srv.URL); err == nil || !strings.Contains(err.Error(), "超过上限") {
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
		got, err := api.DownloadRaw(srv.URL)
		if err != nil {
			t.Fatalf("下载失败: %v", err)
		}
		if string(got.Buffer) != "hello" || got.Filename != "ok.txt" {
			t.Errorf("下载结果错误: %+v", got)
		}
	})
}

func TestDownloadRawErrorsAndEdges(t *testing.T) {
	api := newAPIClient(silentTestLogger{}, 5000, 1024)

	// 非 2xx
	srv500 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	if _, err := api.DownloadRaw(srv500.URL); err == nil {
		t.Error("非 2xx 应报错")
	}
	srv500.Close()

	// 非法 URL（NewRequest 失败）
	if _, err := api.DownloadRaw("://bad"); err == nil {
		t.Error("非法 URL 应报错")
	}

	// Do 失败（拒连）：先起服务端再关掉
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	srv.Close()
	if _, err := api.DownloadRaw(srv.URL); err == nil {
		t.Error("拒连应报错")
	}
}

// 恰好等于上限应通过（现有用例只测了超限）。
func TestDownloadRawExactLimitOK(t *testing.T) {
	const cap = 64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "64")
		_, _ = w.Write(make([]byte, cap))
	}))
	defer srv.Close()

	api := newAPIClient(silentTestLogger{}, 5000, cap)
	got, err := api.DownloadRaw(srv.URL)
	if err != nil {
		t.Fatalf("恰好等于上限应通过: %v", err)
	}
	if len(got.Buffer) != cap {
		t.Errorf("长度应为 %d, got %d", cap, len(got.Buffer))
	}
}

// 分块（无 Content-Length）成功 + filename* 解析透传。
func TestDownloadRawChunkedAndFilenameStar(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Disposition", `attachment; filename*=UTF-8''%E6%8A%A5%E5%91%8A.pdf`)
		_, _ = w.Write([]byte("hello"))
		w.(http.Flusher).Flush()
	}))
	defer srv.Close()

	api := newAPIClient(silentTestLogger{}, 5000, 1024)
	got, err := api.DownloadRaw(srv.URL)
	if err != nil {
		t.Fatalf("下载失败: %v", err)
	}
	if string(got.Buffer) != "hello" {
		t.Errorf("内容错误: %q", got.Buffer)
	}
	if got.Filename != "报告.pdf" {
		t.Errorf("filename* 应解析为 报告.pdf, got %q", got.Filename)
	}
}

func TestNewAPIClientDefaults(t *testing.T) {
	api := newAPIClient(silentTestLogger{}, 0, 0)
	if api.httpClient.Timeout != time.Duration(DefaultRequestTimeout)*time.Millisecond {
		t.Errorf("默认超时应为 %dms, got %v", DefaultRequestTimeout, api.httpClient.Timeout)
	}
	if api.maxDownloadBytes != DefaultMaxDownloadBytes {
		t.Errorf("默认下载上限应为 %d, got %d", DefaultMaxDownloadBytes, api.maxDownloadBytes)
	}
}
