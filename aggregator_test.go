package go_channel_testing

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBufferFilled(t *testing.T) {
	t.Parallel()

	a := &Aggregator{
		in:         make(chan Message),
		out:        make(chan []Message, 2),
		bufferSize: 2,
	}
	t.Cleanup(func() {
		close(a.in)
		close(a.out)
	})

	a.startAggregator()

	expected := []Message{
		{value: "a"},
		{value: "b"},
	}
	a.in <- expected[0]
	a.in <- expected[1]
	actual := <-a.out

	assert.Equal(t, expected, actual)
}

func TestBufferFilledTwice(t *testing.T) {
	t.Parallel()

	a := &Aggregator{
		in:         make(chan Message, 2),
		out:        make(chan []Message, 2),
		bufferSize: 1,
	}
	t.Cleanup(func() {
		close(a.in)
		close(a.out)
	})

	a.startAggregator()

	expected1 := []Message{
		{value: "a"},
	}
	expected2 := []Message{
		{value: "b"},
	}
	a.in <- expected1[0]
	actual := <-a.out
	assert.Equal(t, expected1, actual)

	a.in <- expected2[0]
	actual = <-a.out
	assert.Equal(t, expected2, actual)
}

func TestFlushOnTimer(t *testing.T) {
	t.Parallel()

	a := &Aggregator{
		in:         make(chan Message, 2),
		out:        make(chan []Message, 2),
		bufferSize: 100,
	}
	t.Cleanup(func() {
		close(a.in)
		close(a.out)
	})

	ticker := time.NewTicker(time.Millisecond * 500)
	a.startAggregatorWithTimer(ticker)

	expected := []Message{
		{value: "a"},
		{value: "b"},
	}
	a.in <- expected[0]
	a.in <- expected[1]

	verifyBatchReceived(t, a.out, expected)
}

func TestLargeBufferFilled(t *testing.T) {
	t.Parallel()

	maxMessages := 1000
	a := &Aggregator{
		in:         make(chan Message),
		out:        make(chan []Message, maxMessages),
		bufferSize: 100,
	}
	t.Cleanup(func() {
		close(a.in)
		close(a.out)
	})

	ticker := &time.Ticker{C: make(chan time.Time)}
	//ticker := time.NewTicker(time.Millisecond * 500)
	a.startAggregatorWithTimer(ticker)

	expected := make([][]Message, maxMessages)
	for i := 0; i < len(expected); i++ {
		expected[i] = make([]Message, 100)
		for j := 0; j < len(expected[i]); j++ {
			expected[i][j] = Message{value: randomString(256)}
			a.in <- expected[i][j]
		}
	}
	fmt.Println("Generated all input")

	verifyBatchesReceived(t, a.out, expected)
}

func verifyBatchReceived(t *testing.T, out chan []Message, expected []Message) {
	require.Eventually(t, func() bool {
		for {
			select {
			case actual := <-out:
				fmt.Printf("received %v\n", actual)
				require.Equal(t, expected, actual)
				return true
			default:
				return false
			}
		}

	}, time.Second*1, time.Millisecond*200)
}

func verifyBatchesReceived(t *testing.T, out chan []Message, expected [][]Message) {
	require.Eventually(t, func() bool {
		index := 0
		for {
			select {
			case actual := <-out:
				fmt.Printf("received batch #%d\n", index)
				require.Equal(t, expected[index], actual)
				index++
				if index == len(expected) {
					return true
				}
			default:
				return false
			}
		}

	}, time.Second*1, time.Millisecond*200)
}

func randomString(length int) string {
	rand.Seed(time.Now().UnixNano())
	b := make([]byte, length+2)
	rand.Read(b)
	return fmt.Sprintf("%x", b)[2 : length+2]
}
