package model

import "time"

type MobileShareRecord struct {
	ID               uint      `json:"id" gorm:"primarykey"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
	StorageID        uint      `json:"storage_id" gorm:"index"`
	StorageMountPath string    `json:"storage_mount_path" gorm:"index"`
	DriveID          string    `json:"drive_id" gorm:"uniqueIndex:idx_mobile_share_source"`
	SourceFileID     string    `json:"source_file_id" gorm:"uniqueIndex:idx_mobile_share_source"`
	SourcePath       string    `json:"source_path" gorm:"index"`
	SourceName       string    `json:"source_name" gorm:"index"`
	SourceType       string    `json:"source_type" gorm:"index"`
	PeriodUnit       int       `json:"period_unit"`
	LinkID           string    `json:"link_id" gorm:"index"`
	ShareURL         string    `json:"share_url" gorm:"type:text"`
	ExtractCode      string    `json:"extract_code"`
	IsValid          bool      `json:"is_valid" gorm:"index"`
	LastError        string    `json:"last_error" gorm:"type:text"`
}

type MobileShareCreateArgs struct {
	SourcePath string `json:"source_path"`
	PeriodUnit int    `json:"period_unit"`
}

type MobileShareLink struct {
	LinkID      string `json:"link_id"`
	ShareURL    string `json:"share_url"`
	ExtractCode string `json:"extract_code"`
	ObjID       string `json:"obj_id"`
}

type MobileShareCreateResult struct {
	Record          *MobileShareRecord `json:"record"`
	Created         bool               `json:"created"`
	Existing        bool               `json:"existing"`
	RequiresConfirm bool               `json:"requires_confirm"`
}
