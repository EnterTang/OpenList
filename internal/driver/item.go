package driver

type Additional interface{}

type Select string

type Item struct {
	Name        string `json:"name"`
	Label       string `json:"label,omitempty"`
	Type        string `json:"type"`
	Default     string `json:"default"`
	Options     string `json:"options"`
	Required    bool   `json:"required"`
	Help        string `json:"help"`
	Group       string `json:"group,omitempty"`
	Collapsed   bool   `json:"collapsed,omitempty"`
	VisibleWhen string `json:"visible_when,omitempty"`
}

type Info struct {
	Common     []Item `json:"common"`
	Additional []Item `json:"additional"`
	Config     Config `json:"config"`
}

type IRootPath interface {
	GetRootPath() string
}

type IRootId interface {
	GetRootId() string
}

type RootPath struct {
	RootFolderPath string `json:"root_folder_path"`
}

type RootID struct {
	RootFolderID string `json:"root_folder_id"`
}

func (r RootPath) GetRootPath() string {
	return r.RootFolderPath
}

func (r *RootPath) SetRootPath(path string) {
	r.RootFolderPath = path
}

func (r RootID) GetRootId() string {
	return r.RootFolderID
}
