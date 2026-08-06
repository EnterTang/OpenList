package _115sy

import (
	"context"
	"path"
	"strings"
)

func (c *Client) GetIDByPath(ctx context.Context, rawPath string) (string, error) {
	return c.GetIDByPathFrom(ctx, "0", rawPath)
}

func (c *Client) GetIDByPathFrom(ctx context.Context, rootCID, rawPath string) (string, error) {
	item, err := c.GetItemByPathFrom(ctx, rootCID, rawPath)
	if err != nil {
		return "", err
	}
	return item.ID, nil
}

func (c *Client) GetItemByPathFrom(ctx context.Context, rootCID, rawPath string) (RemoteItem, error) {
	rootCID = strings.TrimSpace(rootCID)
	if rootCID == "" {
		rootCID = "0"
	}
	cleaned := strings.TrimSpace(rawPath)
	if cleaned == "" {
		cleaned = "/"
	}
	cleaned = path.Clean(cleaned)
	if cleaned == "." || cleaned == "/" {
		return RemoteItem{ID: rootCID, Name: "/", IsDir: true}, nil
	}
	if cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return RemoteItem{}, &ProtocolError{Endpoint: EndpointDirID, Message: "path escapes the configured root"}
	}

	current := rootCID
	var currentItem RemoteItem
	components := strings.Split(strings.Trim(cleaned, "/"), "/")
	for _, component := range components {
		if component == "" || component == "." {
			continue
		}
		key := current + "\x00" + component
		if cached, ok := c.pathItemCacheGet(key); ok {
			current = cached.ID
			currentItem = cached
			continue
		}
		entries, err := c.ListFiles(ctx, current, ListOptions{PageSize: maxFilePageSize})
		if err != nil {
			return RemoteItem{}, err
		}
		var found RemoteItem
		for _, entry := range entries {
			if entry.Name == component {
				found = entry
				break
			}
		}
		if found.ID == "" {
			return RemoteItem{}, &ProtocolError{Endpoint: EndpointDirID, Message: "path component not found"}
		}
		c.pathCachePut(key, found.ID)
		c.pathItemCachePut(key, found)
		current = found.ID
		currentItem = found
	}
	if currentItem.ID == "" {
		currentItem = RemoteItem{ID: current, Name: path.Base(cleaned), IsDir: true}
	}
	return currentItem, nil
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

func (c *Client) pathItemCacheGet(key string) (RemoteItem, bool) {
	c.pathMu.RLock()
	defer c.pathMu.RUnlock()
	value, ok := c.pathItemCache[key]
	return value, ok
}

func (c *Client) pathItemCachePut(key string, value RemoteItem) {
	c.pathMu.Lock()
	defer c.pathMu.Unlock()
	if c.pathItemCache == nil {
		c.pathItemCache = make(map[string]RemoteItem)
	}
	c.pathItemCache[key] = value
}

func (c *Client) invalidatePathCache() {
	c.pathMu.Lock()
	defer c.pathMu.Unlock()
	c.pathCache = make(map[string]string)
	c.pathItemCache = make(map[string]RemoteItem)
}
