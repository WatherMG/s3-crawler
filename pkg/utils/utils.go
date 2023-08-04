package utils

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"s3-crawler/pkg/files"
)

func CreatePath(path string) error {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		err = os.MkdirAll(path, 0777)
		if err != nil {
			return fmt.Errorf("creating path error: %w", err)
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
		return fmt.Sprintf("%.3f TB", fbytes/TB)
	case fbytes >= GB:
		return fmt.Sprintf("%.2f GB", fbytes/GB)
	case fbytes >= MB:
		return fmt.Sprintf("%.1f MB", fbytes/MB)
	case fbytes >= KB:
		return fmt.Sprintf("%.1f KB", fbytes/KB)
	default:
		return fmt.Sprintf("%.0f B", fbytes)
	}
}

func SaveDataToFile(file *files.File) error {
	defer file.ReturnToPool()
	if err := CreatePath(file.Path); err != nil {
		return err
	}

	destinationFile, err := os.OpenFile(filepath.Join(file.Path, file.Name), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
	if err != nil {
		return fmt.Errorf("file: [key: %s, size: %d], path: %s: %w", file.Key, file.Size, file.Path, err)
	}
	defer destinationFile.Close()

	if _, err = file.Data.WriteTo(destinationFile); err != nil {
		return fmt.Errorf("file write error: failed to write buffer to file: %w", err)
	}
	return nil
}

func TimeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	fmt.Print("\u001B[2K\r")
	fmt.Printf("%s took %s\n", name, elapsed.Truncate(time.Millisecond))
}
