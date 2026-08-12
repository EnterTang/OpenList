package _115sy

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	netutil "github.com/OpenListTeam/OpenList/v4/internal/net"
	"github.com/OpenListTeam/OpenList/v4/internal/stream"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
)

const (
	ossEndpoint115                   = "cn-shenzhen.oss.aliyuncs.com"
	ossSecurityTokenHeaderName       = "X-OSS-Security-Token"
	ossUserAgent115                  = "aliyun-sdk-android/2.9.1"
	singleUploadThreshold      int64 = 10 * utils.MB
)

func (c *Client) UploadAvailable(ctx context.Context) (UploadAvailability, error) {
	c.uploadMu.Lock()
	defer c.uploadMu.Unlock()
	if c.upload.available() {
		return c.upload, nil
	}
	var availability UploadAvailability
	if err := c.doJSON(ctx, OperationUploadInfo, ProfileAndroid, http.MethodPost, EndpointUploadInfo, nil, nil, &availability); err != nil {
		return UploadAvailability{}, err
	}
	if !availability.UploadAllowed && strings.TrimSpace(availability.UploadAllowedMsg) != "" {
		return UploadAvailability{}, &BusinessError{Kind: KindBusiness, Message: availability.UploadAllowedMsg, Endpoint: EndpointUploadInfo, Profile: ProfileAndroid}
	}
	if !availability.available() {
		return UploadAvailability{}, &ProtocolError{Endpoint: EndpointUploadInfo, Message: "upload availability response is missing user id or key"}
	}
	c.upload = availability
	return availability, nil
}

func (c *Client) UploadByOSS(ctx context.Context, initResp UploadInitResponse, fileSize int64, file io.Reader, up driver.UpdateProgress) (UploadResult, error) {
	if up == nil {
		up = func(float64) {}
	}
	if initResp.Bucket == "" || initResp.Object == "" {
		return UploadResult{}, &ProtocolError{Endpoint: EndpointUploadInit, Message: "upload init response is missing OSS bucket or object"}
	}
	token, err := c.GetOSSToken(ctx)
	if err != nil {
		return UploadResult{}, err
	}
	ossClient, err := netutil.NewOSSClient(firstNonEmpty(token.Endpoint, ossEndpoint115), token.AccessKeyID, token.AccessKeySecret, oss.SecurityToken(token.SecurityToken))
	if err != nil {
		return UploadResult{}, err
	}
	bucket, err := ossClient.Bucket(initResp.Bucket)
	if err != nil {
		return UploadResult{}, err
	}
	var body []byte
	reader := driver.NewLimitedUploadStream(ctx, &driver.ReaderUpdatingProgress{
		Reader:         &stream.SimpleReaderWithSize{Reader: file, Size: fileSize},
		UpdateProgress: up,
	})
	if err := bucket.PutObject(initResp.Object, reader, append(ossOptions(initResp, token), oss.CallbackResult(&body))...); err != nil {
		return UploadResult{}, err
	}
	return decodeUploadResult(body, initResp.Target)
}

func (c *Client) UploadFileByOSS(ctx context.Context, initResp UploadInitResponse, file model.FileStreamer, up driver.UpdateProgress) (UploadResult, error) {
	if up == nil {
		up = func(float64) {}
	}
	if file.GetSize() <= singleUploadThreshold {
		return c.UploadByOSS(ctx, initResp, file.GetSize(), file, up)
	}
	return c.multipartUpload(ctx, initResp, file, up)
}

func (c *Client) multipartUpload(ctx context.Context, initResp UploadInitResponse, file model.FileStreamer, up driver.UpdateProgress) (UploadResult, error) {
	if up == nil {
		up = func(float64) {}
	}
	if initResp.Bucket == "" || initResp.Object == "" {
		return UploadResult{}, &ProtocolError{Endpoint: EndpointUploadInit, Message: "upload init response is missing OSS bucket or object"}
	}
	token, err := c.GetOSSToken(ctx)
	if err != nil {
		return UploadResult{}, err
	}
	ossClient, err := netutil.NewOSSClient(firstNonEmpty(token.Endpoint, ossEndpoint115), token.AccessKeyID, token.AccessKeySecret, oss.SecurityToken(token.SecurityToken), oss.EnableMD5(true), oss.EnableCRC(true))
	if err != nil {
		return UploadResult{}, err
	}
	bucket, err := ossClient.Bucket(initResp.Bucket)
	if err != nil {
		return UploadResult{}, err
	}
	imur, err := bucket.InitiateMultipartUpload(initResp.Object, oss.SetHeader(ossSecurityTokenHeaderName, token.SecurityToken), oss.UserAgentHeader(ossUserAgent115), oss.EnableSha1(), oss.Sequential())
	if err != nil {
		return UploadResult{}, err
	}
	chunkSize := uploadPartSize(file.GetSize())
	sectionReader, err := stream.NewStreamSectionReader(file, int(chunkSize), &up)
	if err != nil {
		return UploadResult{}, err
	}
	partCount := (file.GetSize() + chunkSize - 1) / chunkSize
	parts := make([]oss.UploadPart, 0, partCount)
	var uploaded int64
	for partNumber := int64(1); partNumber <= partCount; partNumber++ {
		if err := ctx.Err(); err != nil {
			return UploadResult{}, err
		}
		partSize := chunkSize
		if partNumber == partCount {
			partSize = file.GetSize() - (partNumber-1)*chunkSize
		}
		reader, err := sectionReader.GetSectionReader(uploaded, partSize)
		if err != nil {
			return UploadResult{}, err
		}
		part, err := bucket.UploadPart(imur, driver.NewLimitedUploadStream(ctx, reader), partSize, int(partNumber), oss.SetHeader(ossSecurityTokenHeaderName, token.SecurityToken), oss.UserAgentHeader(ossUserAgent115))
		sectionReader.FreeSectionReader(reader)
		if err != nil {
			return UploadResult{}, err
		}
		parts = append(parts, part)
		uploaded += partSize
		if up != nil {
			up(float64(uploaded) * 100 / float64(file.GetSize()))
		}
	}
	var body []byte
	if _, err := bucket.CompleteMultipartUpload(imur, parts, append(ossOptions(initResp, token), oss.CallbackResult(&body))...); err != nil {
		return UploadResult{}, err
	}
	return decodeUploadResult(body, initResp.Target)
}

func (c *Client) GetOSSToken(ctx context.Context) (UploadOSSToken, error) {
	var token UploadOSSToken
	if err := c.doJSON(ctx, OperationUploadOSSToken, ProfileUpload, http.MethodGet, EndpointUploadOSSToken, nil, nil, &token); err != nil {
		return UploadOSSToken{}, err
	}
	if token.StatusCode != "" && token.StatusCode != "200" {
		return UploadOSSToken{}, &BusinessError{Kind: KindAuth, Message: "115 OSS token request failed", Endpoint: EndpointUploadOSSToken, Profile: ProfileUpload}
	}
	if token.AccessKeyID == "" || token.AccessKeySecret == "" {
		return UploadOSSToken{}, &ProtocolError{Endpoint: EndpointUploadOSSToken, Message: "OSS token response is missing credentials"}
	}
	return token, nil
}

func decodeUploadResult(body []byte, parentCID string) (UploadResult, error) {
	var result UploadResult
	if len(strings.TrimSpace(string(body))) == 0 {
		result.State = true
		return result, nil
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return UploadResult{}, &ProtocolError{Endpoint: EndpointUploadInit, Message: "OSS callback response is invalid"}
	}
	if !result.State && result.Code != 0 {
		return UploadResult{}, &BusinessError{Kind: classifyBusinessError(result.Code, result.Message, result.Error), Errno: result.Code, Message: firstNonEmpty(result.Message, result.Error), Endpoint: EndpointUploadInit, Profile: ProfileUpload}
	}
	if result.Errno != 0 {
		return UploadResult{}, &BusinessError{Kind: classifyBusinessError(result.Errno, result.Message, result.Error), Errno: result.Errno, Message: firstNonEmpty(result.Message, result.Error), Endpoint: EndpointUploadInit, Profile: ProfileUpload}
	}
	return result, nil
}

func ossOptions(params UploadInitResponse, token UploadOSSToken) []oss.Option {
	return []oss.Option{
		oss.SetHeader(ossSecurityTokenHeaderName, token.SecurityToken),
		oss.Callback(base64.StdEncoding.EncodeToString([]byte(params.Callback.Callback))),
		oss.CallbackVar(base64.StdEncoding.EncodeToString([]byte(params.Callback.CallbackVar))),
		oss.UserAgentHeader(ossUserAgent115),
	}
}

func uploadPartSize(fileSize int64) int64 {
	partSize := int64(20 * utils.MB)
	if fileSize > 1*utils.TB {
		return 5 * utils.GB
	}
	if fileSize > 768*utils.GB {
		return 109951163
	}
	if fileSize > 512*utils.GB {
		return 82463373
	}
	if fileSize > 384*utils.GB {
		return 54975582
	}
	if fileSize > 256*utils.GB {
		return 41231687
	}
	if fileSize > 128*utils.GB {
		return 27487791
	}
	return partSize
}

func uploadResultObject(result UploadResult, parentCID string) (RemoteItem, error) {
	item := result.RemoteItem(parentCID)
	if item.ID == "" && item.PickCode == "" {
		return RemoteItem{}, fmt.Errorf("upload response did not include file id or pickcode")
	}
	return item, nil
}
