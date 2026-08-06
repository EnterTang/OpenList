package _115sy

import (
	"context"
	"path"
	"strings"
)

func (c *Client) GetIDByPath(ctx context.Context, rawPath string) (string, error) {
	cleaned := strings.TrimSpace(rawPath)
	if cleaned == "" {
		cleaned = "/"
	}
	cleaned = path.Clean(cleaned)
	if cleaned == "." || cleaned == "/" {
		return "0", nil
	}
	if cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return "", &ProtocolError{Endpoint: EndpointDirID, Message: "path escapes the configured root"}
	}

	current := "0"
	for _, component := range strings.Split(strings.Trim(cleaned, "/"), "/") {
		if component == "" || component == "." {
			continue
		}
		key := current + "\x00" + component
		if cached, ok := c.pathCacheGet(key); ok {
			current = cached
			continue
		}
		entries, err := c.ListFiles(ctx, current, ListOptions{PageSize: maxFilePageSize})
		if err != nil {
			return "", err
		}
		found := ""
		for _, entry := range entries {
			if entry.Name == component {
				found = entry.ID
				break
			}
		}
		if found == "" {
			return "", &ProtocolError{Endpoint: EndpointDirID, Message: "path component not found"}
		}
		c.pathCachePut(key, found)
		current = found
	}
	return current, nil
}

func (c *Client) pathCacheGet(key string) (string, bool) {
	c.pathMu.RLock()
	defer c.pathMu.RUnlock()
	value, ok := c.pathCache[key]
	return value, ok
}

func (c *Client) pathCachePut(key, value string) {
	c.pathMu.Lock()
	defer c.pathMu.Unlock()
	if c.pathCache == nil {
		c.pathCache = make(map[string]string)
	}
	c.pathCache[key] = value
}

func (c *Client) invalidatePathCache() {
	c.pathMu.Lock()
	defer c.pathMu.Unlock()
	c.pathCache = make(map[string]string)
}
