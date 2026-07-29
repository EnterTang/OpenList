package plugin

import (
	"bytes"
	"fmt"
	"io"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/stream"
	"github.com/OpenListTeam/OpenList/v4/pkg/http_range"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
)

func TrailerBytes() []byte {
	return append(append([]byte{}, Padding...), MagicTag...)
}

type namedStreamer struct {
	model.FileStreamer
	name string
}

func (s *namedStreamer) GetName() string {
	return s.name
}

func (s *namedStreamer) GetMimetype() string {
	return utils.GetMimeType(s.name)
}

// ProcessStreamer applies AntiHash and/or ISO rename to an upload stream.
// AntiHash clears GetHash/GetFile so drivers recompute SHA256 over the mutated body.
// ISO-only rename preserves hash and underlying file handles.
func ProcessStreamer(in model.FileStreamer, opts ProcessOptions) (model.FileStreamer, error) {
	if in == nil {
		return in, nil
	}
	name := in.GetName()
	if !ShouldProcessUpload(name, opts) {
		return in, nil
	}

	anti := opts.AntiHash
	if anti {
		already, err := streamHasAntiHashTrailer(in)
		if err != nil {
			return in, fmt.Errorf("antihash trailer check: %w", err)
		}
		if already {
			anti = false
		}
	}
	iso := opts.ISORename
	finalName := name
	if iso {
		if next, ok := ISOTargetName(name); ok {
			finalName = next
		} else {
			iso = false
		}
	}
	if !anti && !iso {
		return in, nil
	}

	if !anti && iso {
		return &namedStreamer{FileStreamer: in, name: finalName}, nil
	}

	size := in.GetSize()
	if size < 0 {
		return in, fmt.Errorf("antihash requires known stream size")
	}
	size += TrailerSize

	out := &stream.FileStream{
		Obj: &model.Object{
			Name:     finalName,
			Size:     size,
			Modified: in.ModTime(),
			Ctime:    in.CreateTime(),
			// HashInfo intentionally empty so 139 recomputes post-plugin SHA256.
		},
		Reader:   io.MultiReader(in, bytes.NewReader(TrailerBytes())),
		Mimetype: utils.GetMimeType(finalName),
		Closers:  utils.NewClosers(in),
	}
	if exist := in.GetExist(); exist != nil {
		out.SetExist(exist)
	}
	return out, nil
}

func streamHasAntiHashTrailer(in model.FileStreamer) (bool, error) {
	size := in.GetSize()
	tagLen := int64(len(MagicTag))
	if size >= 0 && size < tagLen {
		return false, nil
	}

	if f := in.GetFile(); f != nil {
		fileSize := size
		if fileSize < 0 {
			// Fall back to seeking the end when stream size is unknown.
			cur, err := f.Seek(0, io.SeekCurrent)
			if err != nil {
				return false, err
			}
			end, err := f.Seek(0, io.SeekEnd)
			if err != nil {
				return false, err
			}
			fileSize = end
			if _, err := f.Seek(cur, io.SeekStart); err != nil {
				return false, err
			}
		}
		if fileSize < tagLen {
			return false, nil
		}
		buf := make([]byte, tagLen)
		if _, err := f.ReadAt(buf, fileSize-tagLen); err != nil {
			return false, err
		}
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return false, err
		}
		return bytes.Equal(buf, MagicTag), nil
	}

	if size < 0 {
		return false, nil
	}
	rc, err := in.RangeRead(http_range.Range{Start: size - tagLen, Length: tagLen})
	if err != nil {
		// Non-range streams: treat as not modified and proceed with append.
		return false, nil
	}
	defer func() {
		if c, ok := rc.(io.Closer); ok {
			_ = c.Close()
		}
	}()
	buf := make([]byte, tagLen)
	if _, err := io.ReadFull(rc, buf); err != nil {
		return false, nil
	}
	return bytes.Equal(buf, MagicTag), nil
}
