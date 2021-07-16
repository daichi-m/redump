package main

import (
	"fmt"
	"io"
	"log"
	"strconv"
	"time"

	redis "github.com/gomodule/redigo/redis"
)

// RedisClient is a thin wrappen over redigo's connection pool and exposes the same method
// as regigo's Conn interface
type RedisClient interface {
	Do(cmd string, args ...interface{}) (interface{}, error)
	Keys(cursor, count int) ([][]byte, int, error)
	io.Closer
}

type redigoRedisClient struct {
	pool *redis.Pool
	*Config
}

var _ RedisClient = &redigoRedisClient{}

func (rc *redigoRedisClient) Do(cmd string, args ...interface{}) (interface{}, error) {
	conn := rc.pool.Get()
	defer conn.Close()
	reply, err := conn.Do(cmd, args...)
	if rc.Verbose {
		log.Printf("Execution completed for %s, %v", cmd, args)
	}
	return reply, err
}

func (rc *redigoRedisClient) Keys(cursor, count int) ([][]byte, int, error) {

	reply, err := rc.Do("SCAN", cursor, "COUNT", count)
	if err != nil {
		return nil, 0, err
	}
	parts, ok := reply.([]interface{})
	if !ok {
		return nil, 0, fmt.Errorf("Reply did not match expected format")
	}
	curbytes, ok := parts[0].([]byte)
	if !ok {
		return nil, 0, fmt.Errorf("Cursor was not integer")
	}
	updatedCursor, err := strconv.Atoi(string(curbytes))
	if err != nil {
		return nil, 0, err
	}

	keys, ok := parts[1].([]interface{})
	if !ok {
		return nil, 0, fmt.Errorf("Keys was not interface{} array")
	}
	bkeys := make([][]byte, 0, len(keys))
	for _, k := range keys {
		bk, ok := k.([]byte)
		if !ok {
			log.Println("Error in converting to byte array")
			continue
		}
		bkeys = append(bkeys, bk)
	}
	return bkeys, updatedCursor, nil
}

func (rc *redigoRedisClient) Close() error {
	return rc.pool.Close()
}

func NewRedisClient(conf *Config) (RedisClient, error) {

	pool := redis.Pool{
		TestOnBorrow: poolTest,
		Dial: func() (redis.Conn, error) {
			return poolDial(conf)
		},
		MaxIdle:         10,
		MaxActive:       50,
		MaxConnLifetime: 2 * time.Minute,
	}
	return &redigoRedisClient{
		Config: conf,
		pool:   &pool,
	}, nil

}

func poolDial(conf *Config) (redis.Conn, error) {
	address := fmt.Sprintf("%s:%d", conf.Host, conf.Port)
	opts := make([]redis.DialOption, 0)
	opts = append(opts, redis.DialClientName("redump"))
	opts = append(opts, redis.DialReadTimeout(30*time.Second))
	opts = append(opts, redis.DialConnectTimeout(30*time.Second))
	opts = append(opts, redis.DialKeepAlive(10*time.Minute))
	opts = append(opts, redis.DialDatabase(conf.Database))
	if len(conf.Auth) > 0 {
		opts = append(opts, redis.DialPassword(conf.Auth))
	}
	conn, err := redis.Dial("tcp", address, opts...)
	if err != nil {
		return nil, err
	}
	if _, err := conn.Do("PING"); err != nil {
		return nil, err
	}
	return conn, nil
}

func poolTest(conn redis.Conn, t time.Time) error {
	if time.Since(t) < 2*time.Minute {
		return nil
	}
	_, err := conn.Do("PING")
	return err
}
