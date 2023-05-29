package go_channel_testing

import (
	"fmt"
	"time"
)

type Message struct {
	value string
}

type Aggregator struct {
	in         chan Message
	out        chan []Message
	bufferSize int
}

func (a *Aggregator) startAggregator() {
	go func() {
		buffer := make([]Message, 0)
		for {
			select {
			case m, channelOpen := <-a.in:
				if channelOpen {
					// channel is open - flush only if the buffer is full
					buffer = append(buffer, m)
					if len(buffer) == a.bufferSize {
						a.out <- buffer
						buffer = make([]Message, 0)
					}
				} else {
					// channel got closed - flush the buffer if isn't empty
					fmt.Println("input channel closed")
					if len(buffer) > 0 {
						a.out <- buffer
						buffer = make([]Message, 0)
						fmt.Println("written output on closure")
					}
					return
				}
			}
		}
	}()
}

func (a *Aggregator) startAggregatorWithTimer(ticker *time.Ticker) {
	go func() {
		buffer := make([]Message, 0)
		defer ticker.Stop()
		for {
			select {
			case m, channelOpen := <-a.in:
				if channelOpen {
					// channel is open - flush only if the buffer is full
					buffer = append(buffer, m)
					if len(buffer) == a.bufferSize {
						a.out <- buffer
						buffer = make([]Message, 0)
						fmt.Println("written output on buffer full")
					}
				} else {
					// channel got closed - flush the buffer if isn't empty
					fmt.Println("input channel closed")
					if len(buffer) > 0 {
						a.out <- buffer
						buffer = make([]Message, 0)
						fmt.Println("written output on closure")
					}
					return
				}
			case <-ticker.C:
				// it's time to flush the buffer
				fmt.Printf("Ticker time, buffer size is %d\n", len(buffer))
				if len(buffer) > 0 {
					a.out <- buffer
					buffer = make([]Message, 0)
				}
			default: // do not block
			}
		}
	}()
}
