package utils

import (
	"time"

	"go.uber.org/zap"
)

func InfiniteRetry(fn func() error) {
	var err error
	var sleepTime = time.Second
	var retryCount int
	for {
		err = fn()
		if err == nil {
			return
		}
		zap.L().Warn("try failed", zap.Int("retry", retryCount), zap.Error(err))
		retryCount++
		time.Sleep(sleepTime)
		sleepTime = min(time.Minute, sleepTime*2)
	}
}
