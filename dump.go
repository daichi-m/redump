package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"sync"
)

type RedisDumper interface {
	Dump() int
	DumpKey(key []byte) ([]byte, error)
	io.Closer
}

var _ RedisDumper = &redisDumperImpl{}

type redisDumperImpl struct {
	*Config
	client  RedisClient
	keys    chan []byte
	data    chan KeyData
	writer  *bufio.Writer
	outfile *os.File
	cancel  context.CancelFunc
}

// Dump is the primary function of RedisDumper, which scans all the keys in keyspace
// and iteratively dumps their binary format to the given outfile
func (d *redisDumperImpl) Dump() int {

	ctx, can := context.WithCancel(context.Background())
	d.cancel = can
	keyWg := new(sync.WaitGroup)
	dumpWg := new(sync.WaitGroup)

	for i := 0; i < 2*runtime.NumCPU(); i++ {
		go d.keyFetch(ctx, keyWg)
		keyWg.Add(1)
	}
	go d.dataDump(ctx, dumpWg)
	dumpWg.Add(1)

	var cursor int = 0
	var counter int = 0
	for {
		keys, c, err := d.client.Keys(cursor, 500)
		cursor = c
		if err != nil {
			log.Println("Error in retrieving keys", err.Error())
			continue
		}
		for _, k := range keys {
			d.keys <- k
			counter++
		}
		if cursor == 0 {
			break
		}
	}
	close(d.keys)
	keyWg.Wait()
	close(d.data)
	dumpWg.Wait()
	return counter
}

// DumpKey sends a DUMP command to redis for the given key and responds back with the
// binary data dump for that key
func (d *redisDumperImpl) DumpKey(key []byte) ([]byte, error) {
	reply, err := d.client.Do("DUMP", key)
	if err != nil {
		return nil, err
	}
	repbytes, ok := reply.([]byte)
	if !ok {
		return nil, fmt.Errorf("Reply was not a byte array")
	}
	return repbytes, err
}

// keyFetch reads each key from the channel and fetches the binary dump associates with
// the key, and pushes the correspoding KeyData into the data channel
func (d *redisDumperImpl) keyFetch(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

outer:
	for {
		select {
		case key, ok := <-d.keys:
			if !ok {
				break outer
			}
			data, err := d.DumpKey(key)
			if err != nil && d.Verbose {
				log.Printf("Error in retrieving for key %s - %s", string(key), err.Error())
			}
			kd := KeyData{key, data}
			d.data <- kd

		case <-ctx.Done():
			break outer
		}
	}
}

// dataDump reads each KeyValue object from the data channel and dumps it to the outfile.
func (d *redisDumperImpl) dataDump(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

outer:
	for {
		select {
		case kd, ok := <-d.data:
			if !ok {
				break outer
			}
			d.writer.WriteString(EncodeKeyData(&kd) + "\n")
		case <-ctx.Done():
			break outer
		}
	}
}

func (d *redisDumperImpl) Close() error {

	if _, ok := <-d.keys; ok {
		close(d.keys)
	}
	if _, ok := <-d.data; ok {
		close(d.data)
	}
	if err := d.writer.Flush(); err != nil {
		return err
	}
	if err := d.outfile.Close(); err != nil {
		return err
	}
	d.cancel()
	return nil
}

func NewRedisDumper(conf *Config, client RedisClient) RedisDumper {

	opts := os.O_CREATE | os.O_RDWR
	if conf.Append {
		opts = opts | os.O_APPEND
	} else {
		opts = opts | os.O_TRUNC
	}
	fl, err := os.OpenFile(conf.DumpFile, opts, 0777)
	if err != nil {
		log.Fatalf("Error in opening dumpfile %s", err.Error())
	}
	d := redisDumperImpl{
		Config:  conf,
		client:  client,
		keys:    make(chan []byte, 500),
		data:    make(chan KeyData, 500),
		outfile: fl,
		writer:  bufio.NewWriter(fl),
	}
	return &d

}
