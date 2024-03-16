package io

import (
	"path/filepath"
	"runtime"
)

func getBasePath() string {
	_, filename, _, _ := runtime.Caller(0)
	return filepath.Dir(filename)
}
