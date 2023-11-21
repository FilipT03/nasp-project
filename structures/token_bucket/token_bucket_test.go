package token_bucket

import (
	"reflect"
	"testing"
	"time"
)

func TestTokenBucket(t *testing.T) {
	maxSize := int64(10)
	interval := int64(1) // 1 second

	tb := NewTokenBucket(maxSize, interval)

	// Test initial conditions
	if tb.maxTokenSize != maxSize {
		t.Errorf("Expected maxTokenSize to be %d, got %d", maxSize, tb.maxTokenSize)
	}

	if tb.tokenCount != maxSize {
		t.Errorf("Expected tokenCount to be %d, got %d", maxSize, tb.tokenCount)
	}

	if tb.timeInterval != interval {
		t.Errorf("Expected timeInterval to be %d, got %d", interval, tb.timeInterval)
	}

	// Test token consumption
	for i := int64(0); i < maxSize; i++ {
		if !tb.CheckTokenCondition() {
			t.Errorf("Expected token available, but it was not.")
		}
	}

	if tb.CheckTokenCondition() {
		t.Errorf("Expected token to be exhausted, but it was available.")
	}

	// Wait for one interval
	time.Sleep(time.Duration(interval) * time.Second)

	// Token should be replenished after one interval
	if !tb.CheckTokenCondition() {
		t.Errorf("Expected token to be available after an interval, but it was not.")
	}
}

func TestNow(t *testing.T) {
	now := Now()
	currentUnixTime := time.Now().Unix()
	if now != currentUnixTime {
		t.Errorf("Expected Now() to return %d, but it returned %d", currentUnixTime, now)
	}
}

func TestIsIntervalOver(t *testing.T) {
	pastTime := time.Now().Unix() - 10
	futureTime := time.Now().Unix() + 10

	if !isIntervalOver(pastTime) {
		t.Errorf("Expected isIntervalOver(%d) to be true, but it was false", pastTime)
	}

	if isIntervalOver(futureTime) {
		t.Errorf("Expected isIntervalOver(%d) to be false, but it was true", futureTime)
	}
}

func TestTokenBucketSerialization(t *testing.T) {
	// Create an instance of TokenBucket
	tb := NewTokenBucket(10, 1)

	// Serialize the TokenBucket
	serializedBytes := tb.Serialize()

	// Deserialize the bytes back into a TokenBucket
	deserializedTB := Deserialize(serializedBytes)

	// Check if the deserialized TokenBucket is equal to the original
	if !reflect.DeepEqual(tb, deserializedTB) {
		t.Errorf("Deserialized TokenBucket is not equal to the original.\nOriginal: %+v\nDeserialized: %+v", tb, deserializedTB)
	}
}
