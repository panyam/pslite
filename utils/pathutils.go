package utils

import (
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"strings"
)

func ExpandUserPath(path string) string {
	if len(path) == 0 {
		return path
	}
	if path[0] == '~' {
		usr, _ := user.Current()
		dir := usr.HomeDir
		if path == "~" {
			path = dir
		} else if strings.HasPrefix(path, "~/") {
			path = filepath.Join(dir, path[2:])
		} else {
			panic(fmt.Errorf("Other user home directory not yet implemented"))
		}
	}
	path, _ = filepath.Abs(path)
	return path
}

func EnsureDirOf(path string) error {
	parent := filepath.Base(path)
	if err := os.MkdirAll(parent, 0777); err != nil {
		return err
	}
	return nil
}
