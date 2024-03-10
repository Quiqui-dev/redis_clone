package main

import (
	"bufio"
	"io"
	"os"
	"sync"
	"time"
)

type Aof struct {
	file   *os.File
	reader *bufio.Reader
	mutex  sync.Mutex
}

func NewAof(path string) (*Aof, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)

	if err != nil {
		return nil, err
	}

	aof := &Aof{
		file:   f,
		reader: bufio.NewReader(f),
	}

	// start routine to sync the file to disk every second

	go func() {
		for {
			aof.mutex.Lock()

			aof.file.Sync()

			aof.mutex.Unlock()

			time.Sleep(time.Second)
		}
	}()

	return aof, nil
}

func (aof *Aof) Close() error {
	aof.mutex.Lock()

	defer aof.mutex.Unlock()

	return aof.file.Close()
}

func (aof *Aof) Write(value Value) error {
	aof.mutex.Lock()
	defer aof.mutex.Unlock()

	_, err := aof.file.Write(value.Marshal())

	if err != nil {
		return err
	}

	return nil
}

func (aof *Aof) Read(fn func(value Value)) error {
	aof.mutex.Lock()
	defer aof.mutex.Unlock()

	aof.file.Seek(0, io.SeekStart)

	reader := NewResp(aof.file)

	for {
		value, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}

			return err
		}

		fn(value)
	}

	return nil
}
