package wecomaibot

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"strings"
	"testing"
)

// encryptForTest 用与 DecryptFile 约定一致的方案（AES-256-CBC，IV = key 前 16 字节，
// PKCS#7 填充）加密，用来做往返验证。
func encryptForTest(t *testing.T, key, plaintext []byte) []byte {
	t.Helper()
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatalf("初始化 cipher 失败: %v", err)
	}
	padLen := aes.BlockSize - len(plaintext)%aes.BlockSize
	padded := append(append([]byte(nil), plaintext...), bytes.Repeat([]byte{byte(padLen)}, padLen)...)
	out := make([]byte, len(padded))
	cipher.NewCBCEncrypter(block, key[:aes.BlockSize]).CryptBlocks(out, padded)
	return out
}

func TestDecryptFileRoundTrip(t *testing.T) {
	key := bytes.Repeat([]byte{0x2a}, 32)
	aesKey := base64.StdEncoding.EncodeToString(key)

	cases := []string{
		"",                       // 空明文：整块都是填充
		"hello",                  // 不足一块
		"0123456789abcdef",       // 恰好一块（需补一整块填充）
		strings.Repeat("x", 200), // 多块
	}
	for _, plaintext := range cases {
		ciphertext := encryptForTest(t, key, []byte(plaintext))
		got, err := DecryptFile(ciphertext, aesKey)
		if err != nil {
			t.Fatalf("解密失败 (plaintext len=%d): %v", len(plaintext), err)
		}
		if string(got) != plaintext {
			t.Errorf("解密结果不一致: got %q, want %q", got, plaintext)
		}
	}
}

// 企微给的 aeskey 可能缺尾部 '=' padding（43 字符），解码需容错，与官方 Python 一致。
func TestDecryptFileAcceptsUnpaddedAesKey(t *testing.T) {
	key := bytes.Repeat([]byte{0x2a}, 32)
	padded := base64.StdEncoding.EncodeToString(key) // 44 字符，含 '='
	unpadded := strings.TrimRight(padded, "=")       // 43 字符，无 padding

	plaintext := "hello unpadded"
	ciphertext := encryptForTest(t, key, []byte(plaintext))

	got, err := DecryptFile(ciphertext, unpadded)
	if err != nil {
		t.Fatalf("无 padding aeskey 应解码成功: %v", err)
	}
	if string(got) != plaintext {
		t.Errorf("解密结果不一致: got %q, want %q", got, plaintext)
	}
}

func TestDecryptFileRejectsInvalidInput(t *testing.T) {
	key := bytes.Repeat([]byte{0x2a}, 32)
	aesKey := base64.StdEncoding.EncodeToString(key)

	cases := []struct {
		name   string
		buf    []byte
		aesKey string
	}{
		{"空密文", nil, aesKey},
		{"空 key", []byte("0123456789abcdef"), ""},
		{"key 不是 base64", []byte("0123456789abcdef"), "!!!not-base64!!!"},
		{"key 长度不对", []byte("0123456789abcdef"), base64.StdEncoding.EncodeToString(key[:16])},
		{"密文不是 block 对齐", []byte("0123456789abcde"), aesKey},
		{"padding 非法（末字节为 0）", make([]byte, aes.BlockSize), aesKey},
	}
	for _, c := range cases {
		if _, err := DecryptFile(c.buf, c.aesKey); err == nil {
			t.Errorf("%s: 应返回错误", c.name)
		}
	}
}

// PKCS#7 填充值上限是块大小 16，不能放宽到 32（否则会接受非法填充并多剥字节）。
func TestTrimPKCS7PaddingRejectsOverSizedPadding(t *testing.T) {
	data := bytes.Repeat([]byte{32}, 32) // 末字节 = 32，超过块大小 16
	if _, err := trimPKCS7Padding(data); err == nil {
		t.Error("padding 值 32 超过块大小 16，应返回错误")
	}
	if _, err := trimPKCS7Padding([]byte{16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16}); err != nil {
		t.Error("合法填充值 16 应被接受")
	}
}

// PKCS#7 填充边界：pad 值 1..16 均合法（剥掉全部填充后为空），0/17/32 非法。
func TestTrimPKCS7PaddingBoundaries(t *testing.T) {
	for p := 1; p <= 16; p++ {
		data := bytes.Repeat([]byte{byte(p)}, p)
		got, err := trimPKCS7Padding(data)
		if err != nil {
			t.Fatalf("pad=%d 应通过: %v", p, err)
		}
		if len(got) != 0 {
			t.Errorf("pad=%d 应剥掉全部填充，剩余 %d 字节", p, len(got))
		}
	}
	for _, p := range []int{0, 17, 32} {
		data := make([]byte, 32)
		data[31] = byte(p)
		if _, err := trimPKCS7Padding(data); err == nil {
			t.Errorf("pad=%d 应报错", p)
		}
	}
}

func TestTrimPKCS7PaddingInconsistentBytes(t *testing.T) {
	if _, err := trimPKCS7Padding([]byte{1, 2, 3, 4, 5, 5, 5, 5}); err == nil {
		t.Error("padding 字节不一致应报错")
	}
}

func TestTrimPKCS7PaddingEmpty(t *testing.T) {
	if _, err := trimPKCS7Padding(nil); err == nil {
		t.Error("空数据应报错")
	}
}

func TestTrimPKCS7PaddingPadLargerThanData(t *testing.T) {
	if _, err := trimPKCS7Padding([]byte{0x10}); err == nil {
		t.Error("pad 值 16 大于数据长度 1，应报错")
	}
}
