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
	Episode          int       `json:"episode"`
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
	Episode   int    `json:"episode"`
}

type ETFArchiveTMDBCandidate struct {
	TMDBID           int64            `json:"tmdb_id"`
	Name             string           `json:"name"`
	OriginalName     string           `json:"original_name"`
	Year             int              `json:"year"`
	MediaType        string           `json:"media_type"`
	Category         string           `json:"category"`
	PosterPath       string           `json:"poster_path"`
	PosterURL        string           `json:"poster_url"`
	GenreIDs         []int            `json:"genre_ids"`
	OriginCountry    []string         `json:"origin_country"`
	OriginalLanguage string           `json:"original_language"`
	Seasons          []TMDBSeasonInfo `json:"seasons,omitempty"`
	SeasonMap        map[int]int      `json:"season_map,omitempty"`
}

type TMDBSeasonInfo struct {
	SeasonNumber int    `json:"season_number"`
	EpisodeCount int    `json:"episode_count"`
	Name         string `json:"name,omitempty"`
}

type ETFManualArchiveMetadata struct {
	TMDBID       int64  `json:"tmdb_id"`
	Name         string `json:"name"`
	OriginalName string `json:"original_name"`
	Year         int    `json:"year"`
	MediaType    string `json:"media_type"`
	Category     string `json:"category"`
	Season       int    `json:"season"`
	StartEpisode int    `json:"start_episode"`
}

type ETFManualArchivePreviewReq struct {
	Path     string                   `json:"path" binding:"required"`
	Metadata ETFManualArchiveMetadata `json:"metadata" binding:"required"`
}

type ETFManualArchiveApplyReq struct {
	Path     string                   `json:"path" binding:"required"`
	Metadata ETFManualArchiveMetadata `json:"metadata" binding:"required"`
	Items    []ETFManualArchiveItem   `json:"items"`
}

type ETFManualArchivePreview struct {
	SourcePath       string                 `json:"source_path"`
	TargetFolderName string                 `json:"target_folder_name"`
	ArchiveRoot      string                 `json:"archive_root"`
	ArchiveDirPath   string                 `json:"archive_dir_path"`
	Items            []ETFManualArchiveItem `json:"items"`
}

type ETFManualArchiveItem struct {
	OriginalName string `json:"original_name"`
	NewName      string `json:"new_name"`
	OriginalPath string `json:"original_path"`
	NewPath      string `json:"new_path"`
	ArchivePath  string `json:"archive_path"`
	SourceName   string `json:"source_name"`
	SourceSize   int64  `json:"source_size"`
	SourceSHA256 string `json:"source_sha256"`
	Season       int    `json:"season"`
	Episode      int    `json:"episode"`
}
