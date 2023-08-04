package printprogress

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
)

type Status struct {
	wg       *sync.WaitGroup
	Context  context.Context
	Delay    time.Duration
	Messages chan string
	WithBar  bool
	StopChan chan struct{}
}

func NewStatusPrinter(ctx context.Context, delay time.Duration, withBar bool) *Status {
	status := &Status{
		Messages: make(chan string),
		Delay:    (delay * time.Millisecond) / 10,
		StopChan: make(chan struct{}),
		wg:       &sync.WaitGroup{},
		Context:  ctx,
		WithBar:  withBar,
	}
	status.wg.Add(1)
	go status.start()
	return status
}

func (status *Status) Send(msg string) {
	status.Messages <- msg
}

func (status *Status) Stop() {
	close(status.StopChan)
	status.wg.Wait()
}

func (status *Status) start() {
	defer status.wg.Done()
	message := ""
	ticker := time.NewTicker(status.Delay)
	defer ticker.Stop()
	cursor := 0
	animation := []rune("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
	var builder strings.Builder

	for {
		select {
		case <-status.Context.Done():
			return
		case <-status.StopChan:
			return
		case msg := <-status.Messages:
			message = msg
		case <-ticker.C:
			if status.WithBar {
				builder.WriteRune(animation[cursor])
				builder.WriteRune(' ')
			}
			builder.WriteString(message)
			fmt.Printf("%s\r", builder.String())
			builder.Reset()
			cursor = (cursor + 1) % len(animation)
		}
	}
}
