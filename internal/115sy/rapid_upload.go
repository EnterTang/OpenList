package _115sy

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/elliptic"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/stream"
	"github.com/OpenListTeam/OpenList/v4/pkg/http_range"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/aead/ecdh"
	"github.com/andreburgaud/crypt2go/ecb"
	"github.com/andreburgaud/crypt2go/padding"
	"github.com/pierrec/lz4/v4"
)

const (
	rapidPreHashSize = 128 * utils.KB
	rapidMD5Salt     = "Qclm8MGWUv59TnrR0XPg"
)

func ComputeUploadHashes(file model.FileStreamer, up *model.UpdateProgress) (UploadHashes, error) {
	if up == nil {
		noop := model.UpdateProgress(func(float64) {})
		up = &noop
	}
	size := file.GetSize()
	preSize := int64(rapidPreHashSize)
	if size < preSize {
		preSize = size
	}
	reader, err := file.RangeRead(http_range.Range{Start: 0, Length: preSize})
	if err != nil {
		return UploadHashes{}, err
	}
	if closer, ok := reader.(io.Closer); ok {
		defer closer.Close()
	}
	preHash, err := utils.HashReader(utils.SHA1, reader)
	if err != nil {
		return UploadHashes{}, err
	}

	fullHash := file.GetHash().GetHash(utils.SHA1)
	if len(fullHash) != utils.SHA1.Width {
		_, fullHash, err = stream.CacheFullAndHash(file, up, utils.SHA1)
		if err != nil {
			return UploadHashes{}, err
		}
	}

	return UploadHashes{
		SHA1:       strings.ToUpper(fullHash),
		PreSHA1:    strings.ToUpper(preHash),
		Size:       size,
		PreHashLen: preSize,
	}, nil
}

func (c *Client) RapidUpload(ctx context.Context, req RapidUploadRequest, file model.FileStreamer) (UploadInitResponse, error) {
	availability, err := c.UploadAvailable(ctx)
	if err != nil {
		return UploadInitResponse{}, err
	}
	if availability.SizeLimit > 0 && req.Size > availability.SizeLimit {
		return UploadInitResponse{}, &BusinessError{
			Kind:     KindParam,
			Message:  "file exceeds 115 upload size limit",
			Endpoint: EndpointUploadInfo,
			Profile:  ProfileAndroid,
		}
	}

	signKey, signVal := "", ""
	for attempt := 0; attempt < 2; attempt++ {
		resp, err := c.uploadInit(ctx, availability, req, signKey, signVal)
		if err != nil {
			return UploadInitResponse{}, err
		}
		resp.SHA1 = req.SHA1
		if resp.needsRangeSignature() {
			signKey = resp.SignKey
			signVal, err = UploadDigestRange(file, resp.SignCheck)
			if err != nil {
				return UploadInitResponse{}, err
			}
			continue
		}
		return resp, nil
	}
	return UploadInitResponse{}, &ProtocolError{Endpoint: EndpointUploadInit, Message: "upload init repeated range verification"}
}

func UploadDigestRange(file model.FileStreamer, rangeSpec string) (string, error) {
	var start, end int64
	if _, err := fmt.Sscanf(strings.TrimSpace(rangeSpec), "%d-%d", &start, &end); err != nil {
		return "", err
	}
	if start < 0 || end < start {
		return "", &ProtocolError{Endpoint: EndpointUploadInit, Message: "invalid signature range"}
	}
	reader, err := file.RangeRead(http_range.Range{Start: start, Length: end - start + 1})
	if err != nil {
		return "", err
	}
	if closer, ok := reader.(io.Closer); ok {
		defer closer.Close()
	}
	value, err := utils.HashReader(utils.SHA1, reader)
	if err != nil {
		return "", err
	}
	return strings.ToUpper(value), nil
}

func (c *Client) uploadInit(ctx context.Context, availability UploadAvailability, req RapidUploadRequest, signKey, signVal string) (UploadInitResponse, error) {
	if err := waitAccountLimiter(ctx, c.accountLimiter); err != nil {
		return UploadInitResponse{}, wrapContextError(err, EndpointUploadInit, ProfileUpload)
	}
	ec, err := newUploadCipher()
	if err != nil {
		return UploadInitResponse{}, err
	}
	now := time.Now().UnixMilli()
	encodedToken, err := ec.encodeToken(now)
	if err != nil {
		return UploadInitResponse{}, err
	}

	fileSize := strconv.FormatInt(req.Size, 10)
	target := "U_1_" + strings.TrimSpace(req.ParentCID)
	form := url.Values{}
	form.Set("appid", "0")
	form.Set("appversion", c.appVersion)
	form.Set("userid", strconv.FormatInt(availability.UserID, 10))
	form.Set("filename", req.FileName)
	form.Set("filesize", fileSize)
	form.Set("fileid", strings.ToUpper(req.SHA1))
	form.Set("target", target)
	form.Set("sig", generateUploadSignature(availability.UserID, availability.UserKey, strings.ToUpper(req.SHA1), target))
	form.Set("topupload", "true")
	form.Set("t", strconv.FormatInt(now, 10))
	form.Set("token", generateUploadToken(availability.UserID, c.appVersion, strings.ToUpper(req.SHA1), strings.ToUpper(req.PreSHA1), strconv.FormatInt(now, 10), fileSize, signKey, signVal))
	if signKey != "" && signVal != "" {
		form.Set("sign_key", signKey)
		form.Set("sign_val", signVal)
	}
	encrypted, err := ec.encrypt([]byte(form.Encode()))
	if err != nil {
		return UploadInitResponse{}, err
	}

	query := url.Values{"k_ec": {encodedToken}}
	reqHTTP, err := c.newRequestWithContext(ctx, ProfileUpload, http.MethodPost, EndpointUploadInit, query, encrypted, "application/x-www-form-urlencoded")
	if err != nil {
		return UploadInitResponse{}, err
	}
	resp, err := c.httpClient.Do(reqHTTP)
	if err != nil {
		if contextErr := normalizedContextError(ctx, err); contextErr != nil {
			return UploadInitResponse{}, wrapContextError(contextErr, EndpointUploadInit, ProfileUpload)
		}
		return UploadInitResponse{}, &NetworkError{Kind: KindNetwork, Method: http.MethodPost, Endpoint: EndpointUploadInit, Profile: ProfileUpload, Err: sanitizeRequestError(err)}
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return UploadInitResponse{}, &NetworkError{Kind: KindNetwork, Method: http.MethodPost, Endpoint: EndpointUploadInit, Profile: ProfileUpload, Err: sanitizeRequestError(err)}
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return UploadInitResponse{}, &HTTPError{Kind: KindHTTP, StatusCode: resp.StatusCode, Endpoint: EndpointUploadInit, Profile: ProfileUpload}
	}

	decrypted, decryptErr := ec.decrypt(body)
	if decryptErr == nil && len(bytes.TrimSpace(decrypted)) > 0 {
		body = decrypted
	}
	var initResp UploadInitResponse
	if err := json.Unmarshal(body, &initResp); err != nil {
		return UploadInitResponse{}, &ProtocolError{Endpoint: EndpointUploadInit, Message: "upload init response is invalid"}
	}
	if err := initResp.businessError(); err != nil {
		return UploadInitResponse{}, err
	}
	return initResp, nil
}

func (r UploadInitResponse) businessError() error {
	if r.ErrorCode != 0 && r.ErrorCode != 701 {
		return &BusinessError{Kind: classifyBusinessError(r.ErrorCode, r.ErrorMsg), Errno: r.ErrorCode, Message: r.ErrorMsg, Endpoint: EndpointUploadInit, Profile: ProfileUpload}
	}
	if r.Errno != 0 || (r.State != nil && !*r.State) {
		return &BusinessError{Kind: classifyBusinessError(r.Errno, r.Error), Errno: r.Errno, Message: r.Error, Endpoint: EndpointUploadInit, Profile: ProfileUpload}
	}
	return nil
}

func generateUploadSignature(userID int64, userKey, fileID, target string) string {
	first := sha1.Sum([]byte(strconv.FormatInt(userID, 10) + fileID + target + "0"))
	second := sha1.Sum([]byte(userKey + hex.EncodeToString(first[:]) + "000000"))
	return strings.ToUpper(hex.EncodeToString(second[:]))
}

func generateUploadToken(userID int64, appVersion, fileID, preID, timestamp, fileSize, signKey, signVal string) string {
	userIDString := strconv.FormatInt(userID, 10)
	userIDMD5 := md5.Sum([]byte(userIDString))
	token := md5.Sum([]byte(rapidMD5Salt + fileID + fileSize + signKey + signVal + userIDString + timestamp + hex.EncodeToString(userIDMD5[:]) + appVersion))
	return hex.EncodeToString(token[:])
}

type uploadCipher struct {
	key    []byte
	iv     []byte
	pubKey []byte
}

var uploadRemotePubKey = []byte{
	0x57, 0xA2, 0x92, 0x57, 0xCD, 0x23, 0x20, 0xE5,
	0xD6, 0xD1, 0x43, 0x32, 0x2F, 0xA4, 0xBB, 0x8A,
	0x3C, 0xF9, 0xD3, 0xCC, 0x62, 0x3E, 0xF5, 0xED,
	0xAC, 0x62, 0xB7, 0x67, 0x8A, 0x89, 0xC9, 0x1A,
	0x83, 0xBA, 0x80, 0x0D, 0x61, 0x29, 0xF5, 0x22,
	0xD0, 0x34, 0xC8, 0x95, 0xDD, 0x24, 0x65, 0x24,
	0x3A, 0xDD, 0xC2, 0x50, 0x95, 0x3B, 0xEE, 0xBA,
}

func newUploadCipher() (*uploadCipher, error) {
	const p224BaseLen = 28
	x := big.NewInt(0).SetBytes(uploadRemotePubKey[:p224BaseLen])
	y := big.NewInt(0).SetBytes(uploadRemotePubKey[p224BaseLen:])
	remotePublic := ecdh.Point{X: x, Y: y}
	p224 := ecdh.Generic(elliptic.P224())
	private, public, err := p224.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, p224BaseLen)
	point, ok := public.(ecdh.Point)
	if !ok {
		return nil, fmt.Errorf("invalid upload public key type")
	}
	point.X.FillBytes(buf)
	if big.NewInt(0).And(point.Y, big.NewInt(1)).Cmp(big.NewInt(1)) == 0 {
		buf = append([]byte{p224BaseLen + 1, 0x03}, buf...)
	} else {
		buf = append([]byte{p224BaseLen + 1, 0x02}, buf...)
	}
	secret := p224.ComputeSecret(private, remotePublic)
	return &uploadCipher{key: secret[:aes.BlockSize], iv: secret[len(secret)-aes.BlockSize:], pubKey: buf}, nil
}

func (c *uploadCipher) encrypt(plainText []byte) ([]byte, error) {
	pad := padding.NewPkcs7Padding(aes.BlockSize)
	data, err := pad.Pad(plainText)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(c.key)
	if err != nil {
		return nil, err
	}
	mode := ecb.NewECBEncrypter(block)
	xorKey := append([]byte(nil), c.iv...)
	cipherText := make([]byte, 0, len(data))
	tmp := make([]byte, 0, aes.BlockSize)
	for i, b := range data {
		tmp = append(tmp, b^xorKey[i%aes.BlockSize])
		if i%aes.BlockSize == aes.BlockSize-1 {
			mode.CryptBlocks(xorKey, tmp)
			cipherText = append(cipherText, xorKey...)
			tmp = tmp[:0]
		}
	}
	return cipherText, nil
}

func (c *uploadCipher) decrypt(cipherText []byte) (text []byte, e error) {
	defer func() {
		if err := recover(); err != nil {
			e = fmt.Errorf("%v", err)
		}
	}()
	cipherText = cipherText[0 : len(cipherText)-len(cipherText)%aes.BlockSize]
	block, err := aes.NewCipher(c.key)
	if err != nil {
		return nil, err
	}
	lz4Block := make([]byte, len(cipherText))
	mode := cipher.NewCBCDecrypter(block, c.iv)
	mode.CryptBlocks(lz4Block, cipherText)
	length := int(lz4Block[0]) + int(lz4Block[1])<<8
	text = make([]byte, 0x2000)
	n, err := lz4.UncompressBlock(lz4Block[2:length+2], text)
	if err != nil {
		return nil, err
	}
	return text[:n], nil
}

func (c *uploadCipher) encodeToken(timestamp int64) (string, error) {
	random, err := rand.Int(rand.Reader, big.NewInt(256))
	if err != nil {
		return "", err
	}
	r1 := byte(random.Uint64())
	random, err = rand.Int(rand.Reader, big.NewInt(256))
	if err != nil {
		return "", err
	}
	r2 := byte(random.Uint64())
	tmp := make([]byte, 0, 48)
	timeBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(timeBytes, uint32(timestamp))
	for i := 0; i < 15; i++ {
		tmp = append(tmp, c.pubKey[i]^r1)
	}
	tmp = append(tmp, []byte{r1, 0x73 ^ r1}...)
	for i := 0; i < 3; i++ {
		tmp = append(tmp, r1)
	}
	for i := 0; i < 4; i++ {
		tmp = append(tmp, r1^timeBytes[3-i])
	}
	for i := 15; i < len(c.pubKey); i++ {
		tmp = append(tmp, c.pubKey[i]^r2)
	}
	tmp = append(tmp, []byte{r2, 0x01 ^ r2}...)
	for i := 0; i < 3; i++ {
		tmp = append(tmp, r2)
	}
	crc := make([]byte, 4)
	binary.BigEndian.PutUint32(crc, crc32.ChecksumIEEE(append([]byte("^j>WD3Kr?J2gLFjD4W2y@"), tmp...)))
	for i := 0; i < 4; i++ {
		tmp = append(tmp, crc[3-i])
	}
	return base64.StdEncoding.EncodeToString(tmp), nil
}
