package main

import (
	"bufio"
	"context"
	"io"
	"log"
	"os"
	"runtime"
	"sync"
)

type RedisRestorer interface {
	Restore() int
	RestoreLine(line string) error
	io.Closer
}

var _ RedisRestorer = &redisRestorerImpl{}

type redisRestorerImpl struct {
	*Config

	infile   *os.File
	reader   *bufio.Reader
	client   RedisClient
	lineChan chan string
	cancel   context.CancelFunc
}

func (r *redisRestorerImpl) Restore() int {

	ctx, can := context.WithCancel(context.Background())
	r.cancel = can
	wg := new(sync.WaitGroup)
	var counter int = 0
	for i := 0; i < 2*runtime.NumCPU(); i++ {
		go r.restoreParallel(ctx, wg)
		wg.Add(1)
	}

	for {
		line, err := r.reader.ReadString('\n')
		if err == io.EOF {
			break
		} else if err != nil {
			log.Println("Error in reading line, skipping", err.Error())
			continue
		}
		r.lineChan <- line
		counter++
	}
	close(r.lineChan)
	wg.Wait()
	return counter
}

func (r *redisRestorerImpl) RestoreLine(line string) error {
	kd, err := DecodeKeyData(line)
	if err != nil {
		return err
	}
	_, err = r.client.Do("RESTORE", kd.key, 0, kd.data, "REPLACE")
	if err != nil {
		return err
	}
	if r.Verbose {
		log.Println("Restored successfully", string(kd.key))
	}
	return nil
}

func (r *redisRestorerImpl) Close() error {
	if _, ok := <-r.lineChan; ok {
		close(r.lineChan)
	}
	if err := r.infile.Close(); err != nil {
		return err
	}
	r.cancel()
	return nil
}

func (r *redisRestorerImpl) restoreParallel(ctx context.Context, wg *sync.WaitGroup) {

	defer wg.Done()
outer:
	for {
		select {
		case line, ok := <-r.lineChan:
			if !ok {
				break outer
			}
			err := r.RestoreLine(line)
			if err != nil {
				log.Println("Failed to restore line", line, err.Error())
			}
		case <-ctx.Done():
			log.Println("Context cancelled, exiting")
			break outer
		}
	}
}

func NewRedisRestorer(conf *Config, client RedisClient) RedisRestorer {
	opts := os.O_RDONLY
	fl, err := os.OpenFile(conf.DumpFile, opts, 0777)
	if err != nil {
		log.Fatalf("Error in opening dumpfile %s", err.Error())
	}
	r := redisRestorerImpl{
		Config:   conf,
		client:   client,
		lineChan: make(chan string, 500),
		infile:   fl,
		reader:   bufio.NewReader(fl),
	}
	return &r

}
