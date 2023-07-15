package printprogress

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"s3-crawler/pkg/configuration"
	"s3-crawler/pkg/files"
	"s3-crawler/pkg/utils"
)

// ProgressPrinter provides an interface for printing progress.
type ProgressPrinter interface {
	PrintProgress(count, downloadedCount uint32, totalBytes, progressBytes int64, averageSpeed, progressRatio float64, activeDownloads int)
	StartProgressTicker(ctx context.Context, data *files.FileCollection, activeFiles *sync.Map, start time.Time)
}

// TextProgressPrinter uses simple text to print the progress.
type TextProgressPrinter struct {
	Delay time.Duration
}

// GraphicalProgressPrinter uses text-based graphics for progress bar
type GraphicalProgressPrinter struct {
	BarLength int
	Delay     time.Duration
}

func (tpp *TextProgressPrinter) PrintProgress(count, downloadedCount uint32, totalBytes, progressBytes int64, averageSpeed, progressRatio float64, activeDownloads int) {
	printProgress("", count, downloadedCount, totalBytes, progressBytes, averageSpeed, progressRatio, activeDownloads)
}

func (gpp *GraphicalProgressPrinter) PrintProgress(count, downloadedCount uint32, totalBytes, progressBytes int64, averageSpeed, progressRatio float64, activeDownloads int) {
	if gpp.BarLength == 0 {
		gpp.BarLength = 20
	}
	prefix := createProgressBar(gpp.BarLength, progressRatio)
	printProgress(prefix, count, downloadedCount, totalBytes, progressBytes, averageSpeed, progressRatio, activeDownloads)
}

// NewPrinter Create factory function
func NewPrinter(cfg *configuration.Configuration) ProgressPrinter {
	if cfg.Progress.WithProgressBar {
		return &GraphicalProgressPrinter{
			BarLength: cfg.Progress.BarSize,
			Delay:     cfg.Progress.Delay,
		}
	}
	return &TextProgressPrinter{
		Delay: cfg.Progress.Delay,
	}
}

// createProgressBar Helper function to create progress bar as a string
func createProgressBar(barLength int, progressRatio float64) string {
	if barLength == 0 {
		barLength = 20
	}
	progressBar := int(progressRatio * float64(barLength))
	var result strings.Builder
	result.WriteString("[") // starting bar
	for i := 0; i < barLength; i++ {
		switch {
		case i < progressBar:
			result.WriteString("█") // completed part of the bar
		case i == progressBar:
			// Add time check here. If the current second is an even number, print the symbol.
			if time.Now().Second()%2 == 0 {
				result.WriteString("░") // currently progressing part of the bar
			} else {
				result.WriteRune(' ') // blinking part of the bar
			} // currently progressing part of the bar
		default:
			result.WriteRune(' ') // not yet completed part of the bar
		}
	}
	result.WriteString("]") // ending bar
	return result.String()
}

// printProgress Common function to print a generic progress result
func printProgress(prefix string, count, downloadedCount uint32, totalBytes, progressBytes int64, averageSpeed, progressRatio float64, activeDownloads int) {
	const minProgressRatio = 0.1 // start calculating ETA after 10% of the download is complete
	var estimatedTimeRemaining string
	if progressRatio > minProgressRatio && averageSpeed > 0 {
		timeRemaining := (float64(totalBytes) - float64(progressBytes)) / averageSpeed
		if timeRemaining > 0 {
			remainingDuration := time.Duration(int64(timeRemaining)) * time.Second
			estimatedTimeRemaining = remainingDuration.String()
		}
	} else {
		estimatedTimeRemaining = "--:--:--"
	}
	result := fmt.Sprintf("%s %s/%s (%.2f%%). ",
		prefix,
		utils.FormatBytes(progressBytes),
		utils.FormatBytes(totalBytes),
		progressRatio*100,
	)
	result += fmt.Sprintf("D\\L: %d/%d. Act: %d. Avg: %s/s. ETR: %s",
		downloadedCount,
		count,
		activeDownloads,
		utils.FormatBytes(int64(averageSpeed)),
		estimatedTimeRemaining,
	)
	fmt.Printf("\u001B[2K\r%s", result)
}

func (tpp *TextProgressPrinter) StartProgressTicker(ctx context.Context, data *files.FileCollection, activeFiles *sync.Map, start time.Time) {
	// The duplicated logic from ProgressTicker moved here
	progressTicker(ctx, data, activeFiles, start, tpp.Delay, tpp)
}

func (gpp *GraphicalProgressPrinter) StartProgressTicker(ctx context.Context, data *files.FileCollection, activeFiles *sync.Map, start time.Time) {
	// The duplicated logic from ProgressTicker moved here
	progressTicker(ctx, data, activeFiles, start, gpp.Delay, gpp)
}

func progressTicker(ctx context.Context, data *files.FileCollection, activeFiles *sync.Map, start time.Time, delay time.Duration, printer ProgressPrinter) {
	if delay < 100 {
		delay = 250
	}
	ticker := time.NewTicker(delay * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			activeDownloads := 0
			activeFiles.Range(func(_, b any) bool {
				if b == true {
					activeDownloads++
				}
				return true
			})
			switch {
			case activeDownloads > 0:
				count, downloadedCount, _, totalBytes, progressBytes, averageSpeed, progressRatio := data.GetStatistics(time.Since(start))
				printer.PrintProgress(
					count,
					downloadedCount,
					totalBytes,
					progressBytes,
					averageSpeed,
					progressRatio,
					activeDownloads,
				)
			default:
				if data.DownloadChan != nil && activeDownloads == 0 {
					for _, r := range `⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏` {
						fmt.Printf("\u001B[2K\r%c Waiting metadata to download from bucket.", r)
						time.Sleep(delay)
					}
				}
			}
		}
	}
}
