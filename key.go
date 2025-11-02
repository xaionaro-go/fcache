package fcache

import (
	"github.com/xaionaro-go/fcache/key"
)

type Uint64 = key.Uint64
type Bytes16 = key.Bytes16
type Bytes32 = key.Bytes32
type String = key.String

type Key[KH KeyHash] = key.Key[KH]
type KeyHash = key.Hash
