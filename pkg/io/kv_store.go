package io

import (
	"log"
	"os"
	"path"
)

type IKeyValueStore interface {
	Put(key string, value string) (err error)
	Del(key string) (err error)
	Get(key string) (value string, err error)
	List() (keys []string, err error)
}

type KeyValueStore struct {
	basePath string
}

func NewKeyValueStore(dbPath string) (store *KeyValueStore) {
	err := os.MkdirAll(dbPath, 0777)
	if err != nil {
		log.Fatalln("newKeyValueStore:", err)
	}
	store = &KeyValueStore{dbPath}
	return
}

func (s *KeyValueStore) getPath(key string) string {
	return path.Join(s.basePath, key)
}

func (s *KeyValueStore) Put(key string, value string) (err error) {
	err = os.WriteFile(s.getPath(key), []byte(value), 0777)
	return
}

func (s *KeyValueStore) Del(key string) (err error) {
	_ = os.Remove(s.getPath(key))
	return nil
}

func (s *KeyValueStore) Get(key string) (value string, err error) {
	bytes, err := os.ReadFile(s.getPath(key))
	if err != nil {
		return
	}
	value = string(bytes)
	return
}

func (s *KeyValueStore) List() (keys []string, err error) {
	files, err := os.ReadDir(s.basePath)
	if err != nil {
		return nil, err
	}
	keys = make([]string, len(files))
	for i, file := range files {
		keys[i] = file.Name()
	}
	return keys, nil
}
