package token_bucket

import (
	"encoding/binary"
	"time"
)

type TokenBucket struct {
	maxTokenSize int64
	tokenCount   int64
	timeInterval int64
	timeUpdated  int64
}

func NewTokenBucket(maxSize int64, interval int64) *TokenBucket {
	tokenBucket := TokenBucket{
		maxTokenSize: maxSize,
		tokenCount:   maxSize,
		timeInterval: interval,
		timeUpdated:  Now(),
	}
	return &tokenBucket
}
func (TB *TokenBucket) CheckTokenCondition() bool {
	if isIntervalOver(TB.timeUpdated + TB.timeInterval) {
		TB.timeUpdated = Now()
		TB.tokenCount = TB.maxTokenSize
	}
	if TB.tokenCount <= 0 {
		return false
	}
	TB.tokenCount--
	return true
}

func Now() int64 {
	return time.Now().Unix()
}
func isIntervalOver(time int64) bool {
	return time <= Now()
}
func (TB *TokenBucket) Serialize() []byte {
	bytes := make([]byte, 32)
	binary.LittleEndian.PutUint64(bytes[0:8], uint64(TB.maxTokenSize))
	binary.LittleEndian.PutUint64(bytes[8:16], uint64(TB.tokenCount))
	binary.LittleEndian.PutUint64(bytes[16:24], uint64(TB.timeInterval))
	binary.LittleEndian.PutUint64(bytes[24:32], uint64(TB.timeUpdated))
	return bytes
}
func Deserialize(data []byte) *TokenBucket {
	maxTokenSize := int64(binary.LittleEndian.Uint64(data[0:8]))
	tokenCount := int64(binary.LittleEndian.Uint64(data[8:16]))
	timeInterval := int64(binary.LittleEndian.Uint64(data[16:24]))
	timeUpdated := int64(binary.LittleEndian.Uint64(data[24:32]))
	return &TokenBucket{
		maxTokenSize: maxTokenSize,
		tokenCount:   tokenCount,
		timeInterval: timeInterval,
		timeUpdated:  timeUpdated,
	}
}
