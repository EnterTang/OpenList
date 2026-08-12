package common

import (
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/go-cache"
)

func TestParseTokenAcceptsValidJWTAfterTokenCacheReset(t *testing.T) {
	SecretKey = []byte("test-secret")
	oldConf := conf.Conf
	conf.Conf = &conf.Config{TokenExpiresIn: 24}
	t.Cleanup(func() {
		conf.Conf = oldConf
	})

	token, err := GenerateToken(&model.User{Username: "admin", PwdTS: 7})
	if err != nil {
		t.Fatalf("GenerateToken returned error: %v", err)
	}
	invalidatedTokenCache = cache.NewMemCache[bool]()

	claims, err := ParseToken(token)
	if err != nil {
		t.Fatalf("ParseToken returned error after cache reset: %v", err)
	}
	if claims.Username != "admin" || claims.PwdTS != 7 {
		t.Fatalf("claims = %+v, want admin/PwdTS 7", claims)
	}
}

func TestParseTokenRejectsExplicitlyInvalidatedToken(t *testing.T) {
	SecretKey = []byte("test-secret")
	oldConf := conf.Conf
	conf.Conf = &conf.Config{TokenExpiresIn: 24}
	t.Cleanup(func() {
		conf.Conf = oldConf
	})

	token, err := GenerateToken(&model.User{Username: "admin"})
	if err != nil {
		t.Fatalf("GenerateToken returned error: %v", err)
	}
	if err := InvalidateToken(token); err != nil {
		t.Fatalf("InvalidateToken returned error: %v", err)
	}
	if _, err := ParseToken(token); err == nil {
		t.Fatal("ParseToken returned nil error for invalidated token")
	}
}
