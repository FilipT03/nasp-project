package app

import (
	"nasp-project/structures/token_bucket"
	"nasp-project/util"
)

func (kvs *KeyValueStore) rateLimitReached() bool {
	tbBytes, err := kvs.get(util.RateLimiterKey)
	if err != nil { // block request if Token Bucket read fails
		return true
	}

	var tb *token_bucket.TokenBucket = nil
	if tbBytes == nil {
		tb = token_bucket.NewTokenBucket(kvs.config.TokenBucket.MaxTokenSize, kvs.config.TokenBucket.Interval)
	} else {
		tb = token_bucket.Deserialize(tbBytes)
	}

	if tb == nil { // block request in case of error
		return true
	}

	allowed := tb.CheckTokenCondition()

	err = kvs.put(util.RateLimiterKey, tb.Serialize())
	if err != nil { // block request if Token Bucket operation fails
		return true
	}

	return !allowed
}
