package cacher

import (
	"crypto/md5"
	rnd "crypto/rand"
	"encoding/hex"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"s3-crawler/pkg/files"
)

const (
	partSize    = files.ChunkSize
	numFiles    = 100
	maxFileSize = 10 * files.MiB
)

func TestGetFileMD5Hash(t *testing.T) {
	// Create temporary directory for test files
	tempDir, err := os.MkdirTemp(os.TempDir(), "TestGetFileMD5Hash")
	if err != nil {
		t.Fatalf("Error creating temporary directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create test files with random size
	expectedHashes := make(map[string]string)
	for i := 0; i < numFiles; i++ {
		filePath := filepath.Join(tempDir, fmt.Sprintf("file%d.txt", i+1))
		fileSize := rand.Intn(maxFileSize)

		data := make([]byte, fileSize)
		rnd.Read(data)

		if err = os.WriteFile(filePath, data, 0644); err != nil {
			t.Fatalf("Error writing test file: %v", err)
		}

		parts := int((int64(fileSize) / partSize) + 1)
		md5Digest := make([][]byte, 0, parts)

		for j := int64(0); j < int64(fileSize); j += partSize {
			end := j + partSize
			if end > int64(fileSize) {
				end = int64(fileSize)
			}

			hash := md5.Sum(data[j:end])
			md5Digest = append(md5Digest, hash[:])
		}

		if parts == 1 {
			expectedHashes[filePath] = hex.EncodeToString(md5Digest[0])
			continue
		}

		var finalMD5 []byte
		for _, digest := range md5Digest {
			finalMD5 = append(finalMD5, digest...)
		}

		hash := md5.Sum(finalMD5)
		expectedHashes[filePath] = hex.EncodeToString(hash[:]) + "-" + strconv.Itoa(parts)
	}
	for filePath, expectedHash := range expectedHashes {
		hash, err := getHash(filePath, 0)
		if err != nil {
			t.Errorf("Error hasing data for file %s: %v", filePath, err)
			continue
		}

		if hash != expectedHash {
			t.Errorf("Different hashes for file %s. \nWant: %s\nGot:  %s", filePath, expectedHash, hash)
		} else {
			/*fmt.Printf("Want: %s\nGot:  %s\n", expectedHash, hash)
			fmt.Println("----------------------------------------")*/
		}
	}
}

/*func BenchmarkFileMD5Hash(b *testing.B) {
	filePath := "/tmp/upload/data/newFolder_file_44780.html"

	bufSizes := []int{1024, 2048, 4096, 8196, 16384, 32768, 65536, 131072, 262144, 524288, 1048576}

	for _, bufSize := range bufSizes {
		b.Run("bufSize="+strconv.Itoa(bufSize), func(b *testing.B) {
			bufPool := &sync.Pool{
				New: func() interface{} {
					buf := make([]byte, bufSize)
					return &buf
				},
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				getHash(filePath, files.ChunkSize, bufPool)
			}
		})
	}
}*/
