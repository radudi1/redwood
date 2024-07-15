package storage

import (
	"sync"
)

type ChunkPool struct {
	sync.Pool
	chunkSize int
}

func NewChunkPool(chunkSize int) *ChunkPool {
	cp := &ChunkPool{}
	cp.chunkSize = chunkSize
	cp.Pool.New = cp.New
	return cp
}

func (cp *ChunkPool) New() any {
	return make([]byte, cp.chunkSize)
}

func (cp *ChunkPool) Get() []byte {
	return cp.Pool.Get().([]byte)
}

func (cp *ChunkPool) Put(b []byte) {
	cp.Pool.Put(b)
}
