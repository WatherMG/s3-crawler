package downloader

import (
	"context"
	"fmt"
	"log"
	"time"

	"s3-crawler/pkg/files"

	"github.com/schollz/progressbar/v3"
)

func (d *Downloader) DownloadFiles(ctx context.Context, data *files.Objects) {
	start := time.Now()

	// bar := progressbar.NewOptions64(int64(data.GetTotalBytes()),
	// 	progressbar.OptionShowBytes(true),
	// 	progressbar.OptionSetWidth(15),
	// 	progressbar.OptionSetTheme(progressbar.Theme{
	// 		Saucer:        "=",
	// 		SaucerPadding: "-",
	// 		BarStart:      "[",
	// 		BarEnd:        "]",
	// 	}))

	// bar.Describe("Download:")
	var downloadCount int

	for file := range data.Objects {
		d.wg.Add(1)
		// sem <- struct{}{}
		go func(file *files.File /*, bar *progressbar.ProgressBar*/) {
			defer func() {
				// <-sem
				d.wg.Done()
			}()
			progressbar.OptionSetDescription(fmt.Sprintf("Downloading: %s", file.Key))
			// bar.ChangeMax64(file.Size)

			numBytes, err := d.DownloadFile(ctx, file)
			if err != nil {
				log.Printf("DownloadFile error %s: %v", file.Key, err)
			}

			file.ReturnToPool()

			fmt.Printf("download file %s\n", file.Key)
			data.AddBytes(numBytes)
			downloadCount++
			// bar.Add64(numBytes)
		}(file /*bar*/)
	}

	/*// TODO: Добавить функционал выкачки данных в буфер с передачей в канал, для последующей записи на диск из буфера.
	fileName := strings.ReplaceAll(key, "/", "_")
	path := filepath.Join(d.cfg.LocalPath, fileName)
	if d.cfg.Prefix != "" {
		path = filepath.Join(d.cfg.LocalPath, d.cfg.Prefix+"_"+fileName)
	}

	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return 0, err
	}
	defer file.Close()*/
	d.wg.Wait()
	// close(sem)

	elapsed := time.Since(start)
	fmt.Printf("Downloaded all files in %s. Total bytes: %.3fMB\n", elapsed, data.GetTotalBytes())
	fmt.Printf("Average speed = %.3fMBps", data.GetAverageSpeed(elapsed))
}
