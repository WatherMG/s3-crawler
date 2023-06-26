package files

import (
	"math"
	"time"
)

type Files struct {
	Objects    []*Object
	TotalBytes int64
}

type Object struct {
	Key  string
	Size int64
}

func (f *Files) GetTotalBytes() float64 {
	return float64(f.TotalBytes) / math.Pow(1024, 2)
}

func (f *Files) GetAverageSpeed(duration time.Duration) float64 {
	return (float64(f.TotalBytes) / duration.Seconds()) / math.Pow(1024, 2)
}
