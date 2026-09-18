package wecomaibot

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"fmt"
	"strings"
)

// DecryptFile 用 AES-256-CBC 解密文件数据（对应官方 decrypt_file）。
// aesKey 为 Base64 编码的 32 字节密钥，IV 取 key 前 16 字节，解密后手动去除 PKCS#7 填充。
// aesKey 允许缺尾部 '=' padding（企微/Node 侧常见）。
func DecryptFile(encryptedBuffer []byte, aesKey string) ([]byte, error) {
	if len(encryptedBuffer) == 0 {
		return nil, fmt.Errorf("DecryptFile: encryptedBuffer 为空")
	}
	if aesKey == "" {
		return nil, fmt.Errorf("DecryptFile: aesKey 不能为空")
	}

	// 企微/Node 侧 aesKey 可能缺尾部 '=' padding（Node 的 Buffer.from(str,'base64') 容忍
	// 缺失），而 Go 的 StdEncoding 严格要求 4 的倍数。这里先补齐再解码，与官方 Python
	// 的「补 '=' 到 4 的倍数」语义一致，避免 43 字符无 padding 的 key 解码失败。
	if m := len(aesKey) % 4; m != 0 {
		aesKey += strings.Repeat("=", 4-m)
	}
	key, err := base64.StdEncoding.DecodeString(aesKey)
	if err != nil {
		return nil, fmt.Errorf("DecryptFile: aesKey base64 解码失败: %w", err)
	}
	if len(key) != 32 {
		return nil, fmt.Errorf("DecryptFile: aesKey 长度错误，期望 32 字节，实际 %d", len(key))
	}
	if len(encryptedBuffer)%aes.BlockSize != 0 {
		return nil, fmt.Errorf("DecryptFile: 密文长度不是 block 对齐")
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("DecryptFile: 初始化 cipher 失败: %w", err)
	}

	iv := key[:aes.BlockSize]
	decrypted := make([]byte, len(encryptedBuffer))
	mode := cipher.NewCBCDecrypter(block, iv)
	mode.CryptBlocks(decrypted, encryptedBuffer)

	trimmed, err := trimPKCS7Padding(decrypted)
	if err != nil {
		return nil, fmt.Errorf("DecryptFile: 去除填充失败: %w", err)
	}
	return trimmed, nil
}

// maxPKCS7Padding 是 PKCS#7 填充值的上界。官方文档写明「数据采用 PKCS#7 填充至 32 字节
// 的倍数」，官方 Python SDK 也按 pad_len <= 32 校验，故这里对齐官方用 32，而不是 AES 块
// 大小 16——否则会误拒企微按 32 字节填充的合法文件。
const maxPKCS7Padding = 32

func trimPKCS7Padding(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("空数据")
	}
	padLen := int(data[len(data)-1])
	if padLen < 1 || padLen > maxPKCS7Padding || padLen > len(data) {
		return nil, fmt.Errorf("非法 padding 值: %d", padLen)
	}
	for i := len(data) - padLen; i < len(data); i++ {
		if int(data[i]) != padLen {
			return nil, fmt.Errorf("padding 字节不一致")
		}
	}
	return data[:len(data)-padLen], nil
}
