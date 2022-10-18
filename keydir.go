package bitcask

import "sync"

type EntryIndex struct {
	fileID int64
	valueSize uint32
	valuePos uint64
	timestamp uint64
}

type KeyDir struct {
	sync.RWMutex
	kv map[string]*EntryIndex
}

func (k *KeyDir) Get(key string) *EntryIndex {
	return k.kv[key]
}

func (k *KeyDir) Add(key string, idx *EntryIndex) {
	k.Lock()
	defer k.Unlock()

	k.kv[key] = idx
}

func (k *KeyDir) Remove(key string) {
	k.Lock()
	defer k.Unlock()

	delete(k.kv, key)
}