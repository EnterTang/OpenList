package plugin

type ProcessOptions struct {
	AntiHash  bool
	ISORename bool
	Whitelist string
}

func ShouldProcessUpload(name string, opts ProcessOptions) bool {
	if !opts.AntiHash && !opts.ISORename {
		return false
	}
	if name == "" || IsTempIncompleteName(name) {
		return false
	}
	return ExtensionAllowed(name, ParseWhitelist(opts.Whitelist))
}

// ApplyUploadName returns the final upload name after optional ISO rename.
// Whitelist matching uses the original extension.
func ApplyUploadName(name string, opts ProcessOptions) string {
	if !ShouldProcessUpload(name, opts) || !opts.ISORename {
		return name
	}
	if next, ok := ISOTargetName(name); ok {
		return next
	}
	return name
}

// ExpectedUploadSize returns the size that will be uploaded after optional AntiHash.
func ExpectedUploadSize(sourceSize int64, name string, opts ProcessOptions) int64 {
	if sourceSize < 0 || !ShouldProcessUpload(name, opts) || !opts.AntiHash {
		return sourceSize
	}
	return sourceSize + TrailerSize
}
