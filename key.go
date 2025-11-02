package fcache

import (
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"math"
)

const (
	maxShards = math.MaxUint8 + 1
)

type Key string

func (k Key) ToHash() KeyHash {
	return sha512.Sum512_256([]byte(k))
}

type KeyHash [32]byte

func (kh KeyHash) String() string {
	return fmt.Sprintf("%x", kh[:])
}

func (kh KeyHash) toShard() shard {
	return shard(kh[0])
}

func keyHashFromString(s string) (KeyHash, error) {
	if len(s) != 64 {
		return KeyHash{}, fmt.Errorf("invalid key hash string length: %d", len(s))
	}

	v, err := hex.DecodeString(s)
	if err != nil {
		return KeyHash{}, fmt.Errorf("failed to decode key hash string: %w", err)
	}

	var keyHash KeyHash
	copy(keyHash[:], v)
	return keyHash, nil
}

type shard uint8

func (s shard) String() string {
	return fmt.Sprintf("%02x", uint8(s))
}
