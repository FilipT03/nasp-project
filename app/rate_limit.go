package app

import (
	"errors"
	"nasp-project/structures/token_bucket"
	"nasp-project/util"
)

func (kvs *KeyValueStore) rateLimitReached() (bool, error) {
	tbBytes, err := kvs.get(util.RateLimiterKey)
	if err != nil {
		return true, err
	}

	var tb *token_bucket.TokenBucket = nil
	if tbBytes == nil {
		tb = token_bucket.NewTokenBucket(kvs.config.TokenBucket.MaxTokenSize, kvs.config.TokenBucket.Interval)
	} else {
		tb = token_bucket.Deserialize(tbBytes)
	}

	if tb == nil {
		return true, errors.New("token bucket read failed")
	}

	allowed := tb.CheckTokenCondition()

	err = kvs.put(util.RateLimiterKey, tb.Serialize())
	if err != nil {
		return true, err
	}

	return !allowed, nil
}
