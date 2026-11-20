//go:build !dashboard

// Package web supplies optional embedded production assets.
package web

import "io/fs"

func Assets() fs.FS { return nil }
