package model

import "time"

const (
	ETFArchiveStatusSkipped   = "skipped"
	ETFArchiveStatusArchived  = "archived"
	ETFArchiveStatusFailed    = "failed"
	ETFArchiveStatusCorrected = "corrected"
)

type ETFArchiveRecord struct {
	ID               uint      `json:"id" gorm:"primarykey"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
	StorageID        uint      `json:"storage_id" gorm:"index"`
	StorageMountPath string    `json:"storage_mount_path" gorm:"index"`
	SourceName       string    `json:"source_name" gorm:"index"`
	SourcePath       string    `json:"source_path"`
	LocalETFPath     string    `json:"local_etf_path"`
	ArchiveETFPath   string    `json:"archive_etf_path"`
	ArchiveRoot      string    `json:"archive_root"`
	ArchiveEnabled   bool      `json:"archive_enabled"`
	TMDBMatched      bool      `json:"tmdb_matched" gorm:"index"`
	TMDBID           int64     `json:"tmdb_id" gorm:"index"`
	TMDBName         string    `json:"tmdb_name"`
	TMDBYear         int       `json:"tmdb_year"`
	MediaType        string    `json:"media_type"`
	Category         string    `json:"category"`
	Season           int       `json:"season"`
	SourceSize       int64     `json:"source_size"`
	SourceSHA256     string    `json:"source_sha256"`
	Status           string    `json:"status" gorm:"index"`
	Error            string    `json:"error" gorm:"type:text"`
}

type ETFArchiveCorrection struct {
	TMDBID    int64  `json:"tmdb_id"`
	TMDBName  string `json:"tmdb_name"`
	TMDBYear  int    `json:"tmdb_year"`
	MediaType string `json:"media_type"`
	Category  string `json:"category"`
	Season    int    `json:"season"`
}
