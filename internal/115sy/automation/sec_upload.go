package automation

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	sy "github.com/OpenListTeam/OpenList/v4/internal/115sy"
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/http_range"
)

type SecUploadRequest struct {
	ParentCID        string   `json:"parent_cid"`
	Paths            []string `json:"paths"`
	DeleteOnSuccess  bool     `json:"delete_on_success"`
	ShareAfterUpload bool     `json:"share_after_upload"`
}

type SecUploadItem struct {
	SourcePath string `json:"source_path"`
	FID        string `json:"fid,omitempty"`
	PickCode   string `json:"pickcode,omitempty"`
	ShareCode  string `json:"share_code,omitempty"`
	ShareURL   string `json:"share_url,omitempty"`
	Deleted    bool   `json:"deleted"`
	Error      string `json:"error,omitempty"`
}

type SecUploadResult struct {
	Items []SecUploadItem `json:"items"`
}

func SecUpload(ctx context.Context, client *sy.Client, req SecUploadRequest, up driver.UpdateProgress) (SecUploadResult, error) {
	if client == nil {
		return SecUploadResult{}, fmt.Errorf("115-sy client is nil")
	}
	if up == nil {
		up = func(float64) {}
	}
	result := SecUploadResult{Items: make([]SecUploadItem, 0, len(req.Paths))}
	parentCID := strings.TrimSpace(req.ParentCID)
	if parentCID == "" {
		parentCID = "0"
	}
	for _, sourcePath := range req.Paths {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		item := SecUploadItem{SourcePath: sourcePath}
		file, err := os.Open(sourcePath)
		if err != nil {
			item.Error = redact(err.Error())
			result.Items = append(result.Items, item)
			continue
		}
		info, err := file.Stat()
		if err != nil || info.IsDir() {
			_ = file.Close()
			if err == nil {
				err = os.ErrInvalid
			}
			item.Error = redact(err.Error())
			result.Items = append(result.Items, item)
			continue
		}
		stream := &localFileStreamer{File: file, Object: model.Object{ID: sourcePath, Name: filepath.Base(sourcePath), Size: info.Size(), Modified: info.ModTime()}}
		hashes, err := sy.ComputeUploadHashes(stream, nil)
		if err == nil {
			initResp, rapidErr := client.RapidUpload(ctx, sy.RapidUploadRequest{FileName: stream.GetName(), ParentCID: parentCID, Size: hashes.Size, SHA1: hashes.SHA1, PreSHA1: hashes.PreSHA1}, stream)
			if rapidErr != nil {
				err = rapidErr
			} else if matched, matchErr := initResp.RapidMatched(); matchErr != nil {
				err = matchErr
			} else if matched {
				item.FID, item.PickCode = initResp.FileID, initResp.PickCode
			} else {
				upload, uploadErr := client.UploadFileByOSS(ctx, initResp, stream, up)
				err = uploadErr
				if uploadErr == nil {
					remote := upload.RemoteItem(parentCID)
					item.FID, item.PickCode = remote.ID, remote.PickCode
				}
			}
		}
		_ = file.Close()
		if err != nil {
			item.Error = redact(err.Error())
		} else if req.ShareAfterUpload {
			share, shareErr := client.CreateShare(ctx, sy.CreateShareRequest{FileIDs: []string{firstNonEmpty(item.FID, item.PickCode)}})
			if shareErr != nil {
				item.Error = redact(shareErr.Error())
			} else {
				item.ShareCode = share.ShareCode
				item.ShareURL = share.ShareURL
			}
		}
		if item.Error != "" {
			result.Items = append(result.Items, item)
			continue
		}
		if req.DeleteOnSuccess {
			if removeErr := os.Remove(sourcePath); removeErr != nil {
				item.Error = redact(removeErr.Error())
			} else {
				item.Deleted = true
			}
		}
		result.Items = append(result.Items, item)
	}
	return result, nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

type localFileStreamer struct {
	*os.File
	model.Object
}

func (f *localFileStreamer) GetMimetype() string       { return "application/octet-stream" }
func (f *localFileStreamer) NeedStore() bool           { return false }
func (f *localFileStreamer) IsForceStreamUpload() bool { return false }
func (f *localFileStreamer) GetExist() model.Obj       { return nil }
func (f *localFileStreamer) SetExist(model.Obj)        {}
func (f *localFileStreamer) Add(io.Closer)             {}
func (f *localFileStreamer) AddIfCloser(any)           {}
func (f *localFileStreamer) RangeRead(rng http_range.Range) (io.Reader, error) {
	return io.NewSectionReader(f.File, rng.Start, rng.Length), nil
}
func (f *localFileStreamer) CacheFullAndWriter(_ *model.UpdateProgress, writer io.Writer) (model.File, error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	if writer != nil {
		if _, err := io.Copy(writer, f); err != nil {
			return nil, err
		}
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return nil, err
		}
	}
	return f.File, nil
}
func (f *localFileStreamer) GetFile() model.File { return f.File }
