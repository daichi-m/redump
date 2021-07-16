package main

import (
	b64 "encoding/base64"
	"fmt"
	"strings"
)

const SEPARATOR string = "\u0001"

type KeyData struct {
	key  []byte
	data []byte
}

func Encode(kd *KeyData) string {
	key := b64.StdEncoding.EncodeToString(kd.key)
	data := b64.StdEncoding.EncodeToString(kd.data)
	return fmt.Sprintf("%s%s%s", key, SEPARATOR, data)
}

func Decode(str string) (*KeyData, error) {
	parts := strings.Split(str, SEPARATOR)
	if len(parts) != 2 {
		return nil, fmt.Errorf("Cannot split string into key and value")
	}
	key, err := b64.StdEncoding.DecodeString(parts[0])
	if err != nil {
		return nil, err
	}
	data, err := b64.StdEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, err
	}
	return &KeyData{key, data}, nil
}
