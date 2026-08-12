package _115_open

import "github.com/OpenListTeam/OpenList/v4/internal/op"

func persistRefreshedToken(driver *Open115) {
	op.MustSaveDriverStorage(driver)
}
