package dhtcrawler

import (
	"golang.org/x/time/rate"
)

type limiter struct {
	lim *rate.Limiter
}

func newLimiter(tokens int) limiter {
	return limiter{
		lim: rate.NewLimiter(rate.Limit(tokens), tokens*3),
	}
}

func (l limiter) isSaturated() bool {
	return l.lim.Tokens() <= float64(l.lim.Limit())
}

func (l limiter) isOverloaded() bool {
	return l.lim.Tokens() <= float64(l.lim.Limit()/10)
}

func (l limiter) allow() bool {
	return l.lim.Allow()
}

func (l limiter) limit() rate.Limit {
	return l.lim.Limit()
}

func (l limiter) setLimit(limit rate.Limit) {
	l.lim.SetLimit(limit)
	l.lim.SetBurst(int(limit * 3))
}
