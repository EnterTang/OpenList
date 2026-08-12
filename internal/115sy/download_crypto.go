package _115sy

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"math/big"
)

const p115RSABlockSize = 128

var (
	p115RSAModulus, _ = new(big.Int).SetString(
		"8686980c0f5a24c4b9d43020cd2c22703ff3f450756529058b1cf88f09b86021"+
			"36477198a6e2683149659bd122c33592fdb5ad47944ad1ea4d36c6b172aad633"+
			"8c3bb6ac6227502d010993ac967d1aef00f0c8e038de2e4d3bc2ec368af2e9f1"+
			"0a6f1eda4f7262f136420c07c331b871bf139f74f3010e3c4fe57df3afb71683", 16,
	)
	p115RSAExponent = big.NewInt(0x10001)
	p115RSAKey      = []byte{0x8d, 0xa5, 0xa5, 0x8d}
	p115GKeyL       = []byte{0x78, 0x06, 0xad, 0x4c, 0x33, 0x86, 0x5d, 0x18, 0x4c, 0x01, 0x3f, 0x46}
	p115GKTS        = [...]byte{
		0xf0, 0xe5, 0x69, 0xae, 0xbf, 0xdc, 0xbf, 0x8a, 0x1a, 0x45, 0xe8, 0xbe, 0x7d, 0xa6, 0x73, 0xb8,
		0xde, 0x8f, 0xe7, 0xc4, 0x45, 0xda, 0x86, 0xc4, 0x9b, 0x64, 0x8b, 0x14, 0x6a, 0xb4, 0xf1, 0xaa,
		0x38, 0x01, 0x35, 0x9e, 0x26, 0x69, 0x2c, 0x86, 0x00, 0x6b, 0x4f, 0xa5, 0x36, 0x34, 0x62, 0xa6,
		0x2a, 0x96, 0x68, 0x18, 0xf2, 0x4a, 0xfd, 0xbd, 0x6b, 0x97, 0x8f, 0x4d, 0x8f, 0x89, 0x13, 0xb7,
		0x6c, 0x8e, 0x93, 0xed, 0x0e, 0x0d, 0x48, 0x3e, 0xd7, 0x2f, 0x88, 0xd8, 0xfe, 0xfe, 0x7e, 0x86,
		0x50, 0x95, 0x4f, 0xd1, 0xeb, 0x83, 0x26, 0x34, 0xdb, 0x66, 0x7b, 0x9c, 0x7e, 0x9d, 0x7a, 0x81,
		0x32, 0xea, 0xb6, 0x33, 0xde, 0x3a, 0xa9, 0x59, 0x34, 0x66, 0x3b, 0xaa, 0xba, 0x81, 0x60, 0x48,
		0xb9, 0xd5, 0x81, 0x9c, 0xf8, 0x6c, 0x84, 0x77, 0xff, 0x54, 0x78, 0x26, 0x5f, 0xbe, 0xe8, 0x1e,
		0x36, 0x9f, 0x34, 0x80, 0x5c, 0x45, 0x2c, 0x9b, 0x76, 0xd5, 0x1b, 0x8f, 0xcc, 0xc3, 0xb8, 0xf5,
	}
)

func p115RSAEncrypt(data []byte) (string, error) {
	if p115RSAModulus == nil {
		return "", fmt.Errorf("115 RSA modulus is unavailable")
	}

	prepared := p115XOR(data, p115RSAKey)
	reverseBytes(prepared)
	prepared = p115XOR(prepared, p115GKeyL)
	payload := make([]byte, 16+len(prepared))
	copy(payload[16:], prepared)

	encoded := make([]byte, 0, ((len(payload)+116)/117)*p115RSABlockSize)
	for len(payload) > 0 {
		chunkSize := len(payload)
		if chunkSize > p115RSABlockSize-11 {
			chunkSize = p115RSABlockSize - 11
		}
		chunk := payload[:chunkSize]
		padded := make([]byte, p115RSABlockSize)
		padded[0] = 0
		for i := 1; i < 1+(126-len(chunk)); i++ {
			padded[i] = 2
		}
		separator := 127 - len(chunk)
		padded[separator] = 0
		copy(padded[separator+1:], chunk)

		value := new(big.Int).Exp(new(big.Int).SetBytes(padded), p115RSAExponent, p115RSAModulus)
		block := make([]byte, p115RSABlockSize)
		value.FillBytes(block)
		encoded = append(encoded, block...)
		payload = payload[chunkSize:]
	}
	return base64.StdEncoding.EncodeToString(encoded), nil
}

func p115RSADecrypt(encoded string) ([]byte, error) {
	ciphertext, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("decode 115 RSA response: %w", err)
	}
	if len(ciphertext) == 0 || len(ciphertext)%p115RSABlockSize != 0 {
		return nil, fmt.Errorf("invalid 115 RSA response length %d", len(ciphertext))
	}

	decoded, err := p115RSADecryptRaw(ciphertext)
	if err != nil {
		return nil, err
	}
	return p115DecodeResponse(decoded)
}

func p115RSADecryptRaw(ciphertext []byte) ([]byte, error) {
	if len(ciphertext) == 0 || len(ciphertext)%p115RSABlockSize != 0 {
		return nil, fmt.Errorf("invalid 115 RSA response length %d", len(ciphertext))
	}
	decoded := make([]byte, 0, len(ciphertext))
	for offset := 0; offset < len(ciphertext); offset += p115RSABlockSize {
		value := new(big.Int).Exp(
			new(big.Int).SetBytes(ciphertext[offset:offset+p115RSABlockSize]),
			p115RSAExponent,
			p115RSAModulus,
		)
		block := value.Bytes()
		separator := bytes.IndexByte(block, 0)
		if separator < 0 {
			return nil, fmt.Errorf("invalid 115 RSA response padding")
		}
		decoded = append(decoded, block[separator+1:]...)
	}
	return decoded, nil
}

func p115DecodeResponse(decoded []byte) ([]byte, error) {
	if len(decoded) < 16 {
		return nil, fmt.Errorf("115 RSA response is missing the random key")
	}

	keyLength := p115DeriveKey(decoded[:16], 12)
	payload := p115XOR(decoded[16:], keyLength)
	reverseBytes(payload)
	return p115XOR(payload, p115RSAKey), nil
}

func p115DeriveKey(randomKey []byte, size int) []byte {
	key := make([]byte, size)
	length := size * (size - 1)
	index := 0
	for i := range key {
		var randomByte byte
		if i < len(randomKey) {
			randomByte = randomKey[i]
		}
		key[i] = p115GKTS[length] ^ (randomByte + p115GKTS[index])
		length -= size
		index += size
	}
	return key
}

func p115XOR(source, key []byte) []byte {
	if len(source) == 0 || len(key) == 0 {
		return append([]byte(nil), source...)
	}
	result := make([]byte, len(source))
	prefix := len(source) & 3
	for i := 0; i < prefix && i < len(source); i++ {
		result[i] = source[i] ^ key[i]
	}
	for offset := prefix; offset < len(source); offset += len(key) {
		for i := 0; i < len(key) && offset+i < len(source); i++ {
			result[offset+i] = source[offset+i] ^ key[i]
		}
	}
	return result
}

func reverseBytes(data []byte) {
	for left, right := 0, len(data)-1; left < right; left, right = left+1, right-1 {
		data[left], data[right] = data[right], data[left]
	}
}
