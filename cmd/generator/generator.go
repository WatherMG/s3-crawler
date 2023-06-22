package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"
)

func createFile(fileName string, content []byte) {
	f, err := os.Create(fileName)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	_, err = w.Write(content)
	if err != nil {
		fmt.Println(err)
		return
	}

	err = w.Flush()
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Printf("Создан файл: %s\n", fileName)
	totalSize += len(content)
}

func generateContent(fileType string, maxSize int, buf []byte) []byte {
	size := rand.Intn(maxSize)
	dataSep := randString(size, true, buf)
	baseData := randString(size, false, buf)

	var builder bytes.Buffer
	builder.Grow(size)

	switch fileType {
	case "html":
		elements := []string{"h1", "h2", "h3", "p", "ul", "ol"}
		selectedElements := elementsPool.Get().([]string)[:0]
		defer elementsPool.Put(selectedElements)
		for _, element := range elements {
			if rand.Float32() < 0.5 {
				selectedElements = append(selectedElements, element)
			}
		}

		builder.WriteString("<html>\n<body>\n")
		for _, element := range selectedElements {
			switch element {
			case "h1", "h2", "h3":
				builder.WriteString(fmt.Sprintf("<%s>%s</%s>\n", element, baseData[:70], element))
			case "p":
				for i := 0; i < rand.Intn(25)+1; i++ {
					builder.WriteString(fmt.Sprintf("<p>%s</p>\n", dataSep[:size/16]))
				}
			case "ul", "ol":
				builder.WriteString(fmt.Sprintf("<%s>\n", element))
				for i := 0; i < rand.Intn(10)+1; i++ {
					builder.WriteString(fmt.Sprintf("<li>%s</li>\n", baseData[70+i:95+i]))
				}
				builder.WriteString(fmt.Sprintf("</%s>\n", element))
			}
		}
		builder.WriteString("</body>\n</html>")
	case "json":
		fields := []string{"name", "age", "city", "content", "default", "string", "text", "values", "01", "02", "03", "04", "05"}
		selectedFields := fieldsPool.Get().([]string)[:0]
		defer fieldsPool.Put(selectedFields)
		for _, field := range fields {
			if rand.Float32() < 0.5 {
				selectedFields = append(selectedFields, field)
			}
		}
		jsonContent := jsonContentPool.Get().(map[string]interface{})
		defer jsonContentPool.Put(jsonContent)
		for k := range jsonContent {
			delete(jsonContent, k)
		}
		for _, field := range selectedFields {
			switch field {
			case "name":
				jsonContent[field] = baseData[:40]
			case "age":
				jsonContent[field] = rand.Intn(100) + 1
			case "city":
				jsonContent[field] = baseData[40:65]
			default:
				jsonContent[field] = baseData[:size/16]
			}
		}
		jsonData, _ := json.MarshalIndent(jsonContent, "", " ")
		if len(jsonData) > size {
			jsonData = jsonData[:size]
		}
		builder.Write(jsonData)
	default:
		builder.Write(dataSep[:size])
	}

	data := builder.Bytes()
	if len(data) > size {
		data = data[:size]
	}
	return data
}

func randString(n int, sep bool, buf []byte) []byte {
	var letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	s := buf[:n]
	for i := range s {
		if sep {
			if i%60 == 0 {
				s[i] = '\n'
			}
		}
		s[i] = letters[rand.Intn(len(letters))]
	}
	return s
}

var workerPool = make(chan struct{}, runtime.NumCPU()*12)
var totalCount int
var totalSize int

func generateFiles(directory string, count int, pool *sync.Pool) {
	var wg sync.WaitGroup

	for i := 1; i <= count; i++ {
		workerPool <- struct{}{}
		wg.Add(1)
		go func(i int) {
			defer func() { <-workerPool }()
			defer wg.Done()
			totalCount++
			fileName := fmt.Sprintf("file_%d", totalCount)
			fileType := []string{"html", "json", "txt"}[rand.Intn(3)]
			buf := pool.Get().([]byte)
			defer pool.Put(buf)
			content := generateContent(fileType, maxSize, buf)
			createFile(fmt.Sprintf("%s\\%s.%s", directory, fileName, fileType), content)
		}(i)
	}
	wg.Wait()
}

var (
	bufPool         = &sync.Pool{New: func() interface{} { return make([]byte, maxSize) }}
	elementsPool    = &sync.Pool{New: func() interface{} { return make([]string, 0, 6) }}
	fieldsPool      = &sync.Pool{New: func() interface{} { return make([]string, 0, 13) }}
	jsonContentPool = &sync.Pool{New: func() interface{} { return make(map[string]interface{}) }}
	maxSize         = int(1024 * 1024 * *fileSize)
)

var count = flag.Int("count", 10, "Count of files in one dir")
var fileSize = flag.Float64("size", 0.1, "Max filesize")

func main() {
	flag.Parse()
	start := time.Now()
	runtime.GOMAXPROCS(0)
	rand.New(rand.NewSource(time.Now().UnixNano()))
	directoryNames := []string{"documents", "images", "videos", "music", "projects", "downloads", "logs", "data"}
	subdirsNames := []string{"code", "src", "pkg", "img", "json", "protobuf", "kibana", "elasticsearch", "postgresql", "logstash"}

	for _, directory := range directoryNames {
		rndCount := rand.Intn(*count-1) + 1
		dirPath := filepath.Join("./s3_data", directory)
		createDirAndGenerateFiles(dirPath, rndCount, bufPool)

		for i := 0; i < rand.Intn(3)+1; i++ {
			if len(subdirsNames) > 1 && rand.Float32() < 0.5 {
				index := rand.Intn(len(subdirsNames)-1) + 1
				dirPath = filepath.Join(dirPath, subdirsNames[index])
				subdirsNames = append(subdirsNames[:index], subdirsNames[index+1:]...)
				createDirAndGenerateFiles(dirPath, rndCount, bufPool)
			}
		}
	}
	fmt.Printf("elapsed %s, for %d files. Total size: %.3f MB", time.Since(start), totalCount, float64(totalSize)/math.Pow(10, 6))
}

func createDirAndGenerateFiles(dirPath string, count int, bufPool *sync.Pool) {
	os.MkdirAll(dirPath, 0755)
	generateFiles(dirPath, count, bufPool)
}
