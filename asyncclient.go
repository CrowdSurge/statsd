package statsd

import (
	"time"

	"github.com/quipo/statsd/event"
)

// AsyncClient is a wrapper around the standard client
// that sends the metrics to statsd concurrently.
// errors will be sent through the ErrorChan and must be
// handled in the application.
type AsyncClient struct {
	statsd      *StatsdClient
	ErrorChan   chan error
	requestChan chan func() error
}

func NewAsyncClient(addr, prefix string) *AsyncClient {
	c := &AsyncClient{
		statsd:      NewStatsdClient(addr, prefix),
		ErrorChan:   make(chan error, 100),
		requestChan: make(chan func() error, 1000),
	}

	go c.process()

	return c
}

// CreateSocket creates a UDP connection to a StatsD server
func (c *AsyncClient) CreateSocket() error {
	return c.statsd.CreateSocket()
}

// Incr - Increment a counter metric. Often used to note a particular event
func (c *AsyncClient) Incr(stat string, count int64) {
	c.requestChan <- func() error {
		return c.statsd.Incr(stat, count)
	}
}

// Decr - Decrement a counter metric. Often used to note a particular event
func (c *AsyncClient) Decr(stat string, count int64) {
	c.requestChan <- func() error {
		return c.statsd.Decr(stat, count)
	}
}

// Timing - Track a duration event
// the time delta must be given in milliseconds
func (c *AsyncClient) Timing(stat string, delta int64) {
	c.requestChan <- func() error {
		return c.statsd.Timing(stat, delta)
	}
}

// PrecisionTiming - Track a duration event
// the time delta has to be a duration
func (c *AsyncClient) PrecisionTiming(stat string, delta time.Duration) {
	c.requestChan <- func() error {
		return c.statsd.PrecisionTiming(stat, delta)
	}
}

// Gauge - Gauges are a constant data type. They are not subject to averaging,
// and they don’t change unless you change them. That is, once you set a gauge value,
// it will be a flat line on the graph until you change it again. If you specify
// delta to be true, that specifies that the gauge should be updated, not set. Due to the
// underlying protocol, you can't explicitly set a gauge to a negative number without
// first setting it to zero.
func (c *AsyncClient) Gauge(stat string, value int64) {
	c.requestChan <- func() error {
		return c.statsd.Gauge(stat, value)
	}
}

// GaugeDelta -- Send a change for a gauge
// Gauge Deltas are always sent with a leading '+' or '-'. The '-' takes care of itself but the '+' must added by hand
func (c *AsyncClient) GaugeDelta(stat string, value int64) {
	c.requestChan <- func() error {
		return c.statsd.GaugeDelta(stat, value)
	}
}

// FGauge -- Send a floating point value for a gauge
func (c *AsyncClient) FGauge(stat string, value float64) {
	c.requestChan <- func() error {
		return c.statsd.FGauge(stat, value)
	}
}

// FGaugeDelta -- Send a floating point change for a gauge
func (c *AsyncClient) FGaugeDelta(stat string, value float64) {
	c.requestChan <- func() error {
		return c.statsd.FGaugeDelta(stat, value)
	}
}

// Absolute - Send absolute-valued metric (not averaged/aggregated)
func (c *AsyncClient) Absolute(stat string, value int64) {
	c.requestChan <- func() error {
		return c.statsd.Absolute(stat, value)
	}
}

// FAbsolute - Send absolute-valued floating point metric (not averaged/aggregated)
func (c *AsyncClient) FAbsolute(stat string, value float64) {
	c.requestChan <- func() error {
		return c.statsd.FAbsolute(stat, value)
	}
}

// Total - Send a metric that is continously increasing, e.g. read operations since boot
func (c *AsyncClient) Total(stat string, value int64) {
	c.requestChan <- func() error {
		return c.statsd.Total(stat, value)
	}
}

// SendEvent - Sends stats from an event object
func (c *AsyncClient) SendEvent(e event.Event) {
	c.requestChan <- func() error {
		return c.statsd.SendEvent(e)
	}
}

func (c *AsyncClient) process() {
	for {
		req, open := <-c.requestChan

		if !open {
			return
		}

		err := req()
		if err != nil {
			c.ErrorChan <- err
		}
	}
}

func (c *AsyncClient) Close() error {
	err := c.statsd.Close()
	if err != nil {
		return err
	}
	close(c.requestChan)
	return nil
}
