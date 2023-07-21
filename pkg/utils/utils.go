package utils

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func CreatePath(path string) error {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		err = os.MkdirAll(path, os.ModePerm)
		if err != nil {
			log.Printf("MkdirAll error: %v", err)
			return err
		}
	}
	return nil
}

func HasValidExtension(name string, extensions []string) bool {
	ext := filepath.Ext(name)

	if len(extensions) == 0 || (len(extensions) == 1 && extensions[0] == "") {
		return true
	}

	for _, validExt := range extensions {
		if ext == validExt || (ext != "" && ext[1:] == validExt) {
			return true
		}
	}
	return false
}

func HasValidName(name, nameMask string) bool {
	return strings.Contains(name, nameMask)
}

func HasValidSize(fileSize, minSize, maxSize int64) bool {
	return (minSize == 0 || fileSize >= minSize) && (maxSize == 0 || fileSize <= maxSize)
}

func FormatBytes(bytes int64) string {
	fbytes := float64(bytes)
	const (
		_ float64 = 1 << (10 * iota)
		KB
		MB
		GB
		TB
	)
	switch {
	case fbytes >= TB:
		return fmt.Sprintf("%.2f TB", fbytes/TB)
	case fbytes >= GB:
		return fmt.Sprintf("%.1f GB", fbytes/GB)
	case fbytes >= MB:
		return fmt.Sprintf("%.0f MB", fbytes/MB)
	case fbytes >= KB:
		return fmt.Sprintf("%.0f KB", fbytes/KB)
	default:
		return fmt.Sprintf("%.0f B", fbytes)
	}
}

func TimeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	fmt.Printf("%s took %s\n", name, elapsed)
}
