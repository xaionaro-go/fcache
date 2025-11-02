package key

import (
	"crypto/sha512"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"unsafe"
)

const (
	MaxShards = math.MaxUint8 + 1
)

type Key[KH Hash] interface {
	ToHash() KH
}

type AbstractHash interface {
	fmt.Stringer
	ToShard() Shard
}

type AbstractHashPtr interface {
	AbstractHash
	SetFromString(s string) error
}

type Hash interface {
	comparable
	AbstractHash
}

type HashPtr[KH Hash] interface {
	*KH
	AbstractHashPtr
}

type Uint64 uint64

func (k Uint64) ToHash() Uint64 {
	return k
}

func (kh Uint64) String() string {
	return fmt.Sprintf("%016x", uint64(kh))
}

func (kh *Uint64) SetFromString(s string) error {
	v, err := keyHashFromString[Uint64](s)
	if err != nil {
		return err
	}
	*kh = Uint64(binary.BigEndian.Uint64(v))
	return nil
}

func (kh Uint64) ToShard() Shard {
	return Shard(kh % MaxShards)
}

type Bytes16 [16]byte

func (k Bytes16) ToHash() Bytes16 {
	return k
}

func (kh Bytes16) String() string {
	return fmt.Sprintf("%x", kh[:])
}

func (kh *Bytes16) SetFromString(s string) error {
	v, err := keyHashFromString[Bytes16](s)
	if err != nil {
		return err
	}
	copy(kh[:], v[:])
	return nil
}

func (kh Bytes16) ToShard() Shard {
	return Shard(kh[0])
}

type Bytes32 [32]byte

func (k Bytes32) ToHash() Bytes32 {
	return k
}

func (kh Bytes32) String() string {
	return fmt.Sprintf("%x", kh[:])
}

func (kh *Bytes32) SetFromString(s string) error {
	v, err := keyHashFromString[Bytes32](s)
	if err != nil {
		return err
	}
	copy(kh[:], v[:])
	return nil
}

func (kh Bytes32) ToShard() Shard {
	return Shard(kh[0])
}

func keyHashFromString[KH Hash](s string) ([]byte, error) {
	var zero KH
	if len(s) != int(unsafe.Sizeof(zero))*2 {
		return nil, fmt.Errorf("invalid key hash string length: %d", len(s))
	}

	v, err := hex.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("failed to decode key hash string: %w", err)
	}

	return v, nil
}

type String string

func (k String) ToHash() Bytes32 {
	return sha512.Sum512_256([]byte(k))
}

type Shard uint8

func (s Shard) String() string {
	return fmt.Sprintf("%02x", uint8(s))
}
