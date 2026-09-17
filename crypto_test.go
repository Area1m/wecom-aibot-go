package wecomaibot

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"strings"
	"testing"
)

// encryptForTest 用与 decryptFile 约定一致的方案（AES-256-CBC，IV = key 前 16 字节，
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
		got, err := decryptFile(ciphertext, aesKey)
		if err != nil {
			t.Fatalf("解密失败 (plaintext len=%d): %v", len(plaintext), err)
		}
		if string(got) != plaintext {
			t.Errorf("解密结果不一致: got %q, want %q", got, plaintext)
		}
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
		if _, err := decryptFile(c.buf, c.aesKey); err == nil {
			t.Errorf("%s: 应返回错误", c.name)
		}
	}
}
