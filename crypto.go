package wecomaibot

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"fmt"
)

func decryptFile(encryptedBuffer []byte, aesKey string) ([]byte, error) {
	if len(encryptedBuffer) == 0 {
		return nil, fmt.Errorf("decryptFile: encryptedBuffer 为空")
	}
	if aesKey == "" {
		return nil, fmt.Errorf("decryptFile: aesKey 不能为空")
	}

	key, err := base64.StdEncoding.DecodeString(aesKey)
	if err != nil {
		return nil, fmt.Errorf("decryptFile: aesKey base64 解码失败: %w", err)
	}
	if len(key) != 32 {
		return nil, fmt.Errorf("decryptFile: aesKey 长度错误，期望 32 字节，实际 %d", len(key))
	}
	if len(encryptedBuffer)%aes.BlockSize != 0 {
		return nil, fmt.Errorf("decryptFile: 密文长度不是 block 对齐")
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("decryptFile: 初始化 cipher 失败: %w", err)
	}

	iv := key[:aes.BlockSize]
	decrypted := make([]byte, len(encryptedBuffer))
	mode := cipher.NewCBCDecrypter(block, iv)
	mode.CryptBlocks(decrypted, encryptedBuffer)

	trimmed, err := trimPKCS7Padding32(decrypted)
	if err != nil {
		return nil, fmt.Errorf("decryptFile: 去除填充失败: %w", err)
	}
	return trimmed, nil
}

func trimPKCS7Padding32(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("空数据")
	}
	padLen := int(data[len(data)-1])
	if padLen < 1 || padLen > 32 || padLen > len(data) {
		return nil, fmt.Errorf("非法 padding 值: %d", padLen)
	}
	for i := len(data) - padLen; i < len(data); i++ {
		if int(data[i]) != padLen {
			return nil, fmt.Errorf("padding 字节不一致")
		}
	}
	return data[:len(data)-padLen], nil
}
