// Copyright 2026 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package healthcheck

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/raft/v3"
)

const (
	CheckLinearRead       = "etcd.health/linear_read"
	CheckSerializableRead = "etcd.health/serializable_read"
	CheckApplyLag         = "etcd.health/apply_lag"
	CheckApplyStall       = "etcd.health/apply_stall"
	CheckNoLeader         = "etcd.health/no_leader"
	CheckDataCorruption   = "etcd.health/data_corruption"
	CheckLearner          = "etcd.health/learner"
	CheckDefrag           = "etcd.health/defrag"

	allServices = ""
)

// ServerStateReader abstracts etcdserver state inspection without circular imports.
type ServerStateReader interface {
	Lead() uint64
	MemberID() types.ID
	CommittedIndex() uint64
	AppliedIndex() uint64
	LinearizableReadNotify(ctx context.Context) error
	SerializableReadCheck(ctx context.Context) error
	IsLearner() bool
	Alarms() []*pb.AlarmMember
	ElectionTimeout() time.Duration
}

// Config defines the tuning parameters for the health checker.
type Config struct {
	Interval          time.Duration
	Timeout           time.Duration
	FailureThreshold  int
	SuccessThreshold  int
	ApplyLagThreshold uint64
}

// Options provides hooks and overrides for testing and gate interactions.
type Options struct {
	Logger                      *zap.Logger
	OnProbeStart                func()
	OnProbeComplete             func()
	SlowProbeDelay              time.Duration
	GracePeriodOverride         time.Duration
	ServerHealthCheckEnabled    bool
	StopGRPCServiceOnDefragGate bool
}

type checkTracker struct {
	status           healthpb.HealthCheckResponse_ServingStatus
	consecutiveFails int
	consecutiveSucc  int
}

// ServerHealthChecker evaluates the 8 health checks periodically and coordinates gRPC health status.
type ServerHealthChecker struct {
	server ServerStateReader
	cfg    Config
	opts   Options
	lg     *zap.Logger

	mu            sync.Mutex
	healthServers []*health.Server

	trackers map[string]*checkTracker

	defragCount atomic.Int64
	probing     atomic.Bool

	prevCommitted uint64
	prevApplied   uint64
	stallCount    int

	leaderLossStartTime time.Time
	aggregateStatus     healthpb.HealthCheckResponse_ServingStatus

	stopC chan struct{}
}

// NewServerHealthChecker creates a new health checker instance.
func NewServerHealthChecker(srv ServerStateReader, cfg Config, opts Options) *ServerHealthChecker {
	initMetricsOnce()

	if cfg.Interval <= 0 {
		cfg.Interval = 500 * time.Millisecond
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 5 * time.Second
	}
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = 3
	}
	if cfg.SuccessThreshold <= 0 {
		cfg.SuccessThreshold = 2
	}
	if cfg.ApplyLagThreshold == 0 {
		cfg.ApplyLagThreshold = 5000
	}

	lg := opts.Logger
	if lg == nil {
		lg = zap.NewNop()
	}

	checks := []string{
		CheckLinearRead,
		CheckSerializableRead,
		CheckApplyLag,
		CheckApplyStall,
		CheckNoLeader,
		CheckDataCorruption,
		CheckLearner,
		CheckDefrag,
	}

	trackers := make(map[string]*checkTracker, len(checks))
	for _, c := range checks {
		trackers[c] = &checkTracker{
			status: healthpb.HealthCheckResponse_SERVING,
		}
	}

	return &ServerHealthChecker{
		server:          srv,
		cfg:             cfg,
		opts:            opts,
		lg:              lg,
		trackers:        trackers,
		aggregateStatus: healthpb.HealthCheckResponse_SERVING,
		stopC:           make(chan struct{}),
	}
}

// RegisterHealthServer registers a gRPC health.Server instance to receive status updates.
func (c *ServerHealthChecker) RegisterHealthServer(hs *health.Server) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.healthServers = append(c.healthServers, hs)

	// Set initial statuses blindly
	for checkName, t := range c.trackers {
		hs.SetServingStatus(checkName, t.status)
	}
	hs.SetServingStatus(allServices, c.aggregateStatus)
}

// Run starts the periodic health-check evaluation loop.
func (c *ServerHealthChecker) Run(ctx context.Context) {
	ticker := time.NewTicker(c.cfg.Interval)
	defer func() {
		ticker.Stop()
		c.mu.Lock()
		for _, hs := range c.healthServers {
			hs.Shutdown()
		}
		c.mu.Unlock()
	}()

	// Execute initial probe immediately
	c.probePass(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopC:
			return
		case <-ticker.C:
			c.probePass(ctx)
		}
	}
}

// Stop terminates the checker.
func (c *ServerHealthChecker) Stop() {
	select {
	case <-c.stopC:
	default:
		close(c.stopC)
	}
}

// DefragStarted tracks an active defragmentation call and immediately sets health status.
func (c *ServerHealthChecker) DefragStarted() {
	c.defragCount.Add(1)
	c.applyImmediateCheck(CheckDefrag, false)
}

// DefragFinished decrements the active defragmentation counter.
func (c *ServerHealthChecker) DefragFinished() {
	val := c.defragCount.Add(-1)
	if val < 0 {
		c.defragCount.Store(0)
		val = 0
	}
	if val == 0 {
		c.applyImmediateCheck(CheckDefrag, true)
	}
}

// ActiveDefragCount returns the count of running defragmentation calls.
func (c *ServerHealthChecker) ActiveDefragCount() int64 {
	return c.defragCount.Load()
}

func (c *ServerHealthChecker) applyImmediateCheck(checkName string, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	t, exists := c.trackers[checkName]
	if !exists {
		return
	}

	if ok {
		t.status = healthpb.HealthCheckResponse_SERVING
		t.consecutiveFails = 0
		t.consecutiveSucc = c.cfg.SuccessThreshold
	} else {
		t.status = healthpb.HealthCheckResponse_NOT_SERVING
		t.consecutiveSucc = 0
		t.consecutiveFails = c.cfg.FailureThreshold
	}

	for _, hs := range c.healthServers {
		hs.SetServingStatus(checkName, t.status)
	}

	c.recomputeAggregateLocked(time.Now())
}

func (c *ServerHealthChecker) probePass(ctx context.Context) {
	if !c.probing.CompareAndSwap(false, true) {
		healthCheckSuppressedTicks.Inc()
		return
	}
	defer c.probing.Store(false)

	if c.opts.OnProbeStart != nil {
		c.opts.OnProbeStart()
	}
	if c.opts.OnProbeComplete != nil {
		defer c.opts.OnProbeComplete()
	}

	if c.opts.SlowProbeDelay > 0 {
		select {
		case <-time.After(c.opts.SlowProbeDelay):
		case <-ctx.Done():
			return
		}
	}

	probeCtx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()

	start := time.Now()

	// 1. Check NoLeader
	lead := c.server.Lead()
	hasLeader := lead != uint64(raft.None) && lead != 0

	// 2. Check LinearRead
	var linearReadErr error
	if c.server != nil {
		linearReadErr = c.server.LinearizableReadNotify(probeCtx)
	}

	// 3. Check SerializableRead
	var serReadErr error
	if c.server != nil {
		serReadErr = c.server.SerializableReadCheck(probeCtx)
	}

	// 4. Check Indices (apply lag & apply stall)
	ci := c.server.CommittedIndex()
	ai := c.server.AppliedIndex()

	var lag uint64
	if ci > ai {
		lag = ci - ai
	}
	healthCheckApplyLag.Set(float64(lag))

	applyLagOK := lag <= c.cfg.ApplyLagThreshold

	// 5. Apply stall detection
	// Work is pending (ci > ai) AND applied index did not advance (ai <= prevApplied)
	applyStall := false
	if ci > ai && ai <= c.prevApplied {
		c.stallCount++
		if c.stallCount >= c.cfg.FailureThreshold {
			applyStall = true
		}
	} else {
		c.stallCount = 0
	}
	c.prevCommitted = ci
	c.prevApplied = ai

	// 6. Check DataCorruption
	localID := uint64(c.server.MemberID())
	corrupt := false
	for _, a := range c.server.Alarms() {
		if a.Alarm == pb.AlarmType_CORRUPT {
			if a.MemberID == localID || a.MemberID == 0 {
				corrupt = true
				break
			}
		}
	}

	// 7. Check Learner
	isLearner := c.server.IsLearner()
	if isLearner {
		healthCheckLearnerState.Set(1)
	} else {
		healthCheckLearnerState.Set(0)
	}

	// 8. Check Defrag
	isDefragging := c.defragCount.Load() > 0

	results := map[string]bool{
		CheckNoLeader:         hasLeader,
		CheckLinearRead:       linearReadErr == nil,
		CheckSerializableRead: serReadErr == nil,
		CheckApplyLag:         applyLagOK,
		CheckApplyStall:       !applyStall,
		CheckDataCorruption:   !corrupt,
		CheckLearner:          !isLearner,
		CheckDefrag:           !isDefragging,
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for checkName, ok := range results {
		c.updateCheckLocked(checkName, ok)
	}

	c.recomputeAggregateLocked(start)

	healthCheckLastTick.Set(float64(time.Now().Unix()))
	healthCheckDuration.Observe(time.Since(start).Seconds())
}

func (c *ServerHealthChecker) updateCheckLocked(name string, ok bool) {
	t := c.trackers[name]
	oldStatus := t.status

	if ok {
		t.consecutiveFails = 0
		t.consecutiveSucc++
		if t.status == healthpb.HealthCheckResponse_NOT_SERVING && t.consecutiveSucc >= c.cfg.SuccessThreshold {
			t.status = healthpb.HealthCheckResponse_SERVING
			c.logTransition(name, oldStatus, t.status)
		}
	} else {
		healthCheckFailures.WithLabelValues(name).Inc()
		t.consecutiveSucc = 0
		t.consecutiveFails++
		if t.status == healthpb.HealthCheckResponse_SERVING && t.consecutiveFails >= c.cfg.FailureThreshold {
			t.status = healthpb.HealthCheckResponse_NOT_SERVING
			c.logTransition(name, oldStatus, t.status)
		}
	}

	for _, hs := range c.healthServers {
		hs.SetServingStatus(name, t.status)
	}

	if t.status == healthpb.HealthCheckResponse_SERVING {
		healthCheckState.WithLabelValues(name).Set(1)
	} else {
		healthCheckState.WithLabelValues(name).Set(0)
	}
}

func (c *ServerHealthChecker) recomputeAggregateLocked(now time.Time) {
	lead := c.server.Lead()
	hasLeader := lead != uint64(raft.None) && lead != 0

	// Grace period evaluation for linear read
	linearReadTracker := c.trackers[CheckLinearRead]
	linearReadFailed := linearReadTracker.status == healthpb.HealthCheckResponse_NOT_SERVING

	linearReadFatal := false
	if linearReadFailed {
		if hasLeader {
			// Leader exists but linear read failed for threshold ticks -> local problem!
			linearReadFatal = true
			c.leaderLossStartTime = time.Time{}
		} else {
			// No leader
			if c.leaderLossStartTime.IsZero() {
				c.leaderLossStartTime = now
			}
			gracePeriod := c.opts.GracePeriodOverride
			if gracePeriod == 0 {
				electionTimeout := c.server.ElectionTimeout()
				gracePeriod = 10 * electionTimeout
				if gracePeriod < 30*time.Second {
					gracePeriod = 30 * time.Second
				}
			}
			if now.Sub(c.leaderLossStartTime) > gracePeriod {
				linearReadFatal = true
			}
		}
	} else {
		if hasLeader {
			c.leaderLossStartTime = time.Time{}
		}
	}

	// Member-local fatal conditions:
	// 1. Sustained linear read failure after grace period
	// 2. Apply stall
	// 3. Local CORRUPT alarm
	// 4. Learner status
	// 5. Active defragmentation
	applyStallFatal := c.trackers[CheckApplyStall].status == healthpb.HealthCheckResponse_NOT_SERVING
	corruptionFatal := c.trackers[CheckDataCorruption].status == healthpb.HealthCheckResponse_NOT_SERVING
	learnerFatal := c.trackers[CheckLearner].status == healthpb.HealthCheckResponse_NOT_SERVING
	defragFatal := c.trackers[CheckDefrag].status == healthpb.HealthCheckResponse_NOT_SERVING

	newAggregate := healthpb.HealthCheckResponse_SERVING
	if linearReadFatal || applyStallFatal || corruptionFatal || learnerFatal || defragFatal {
		newAggregate = healthpb.HealthCheckResponse_NOT_SERVING
	}

	if newAggregate != c.aggregateStatus {
		c.logTransition(allServices, c.aggregateStatus, newAggregate)
		c.aggregateStatus = newAggregate
		for _, hs := range c.healthServers {
			hs.SetServingStatus(allServices, c.aggregateStatus)
		}
		if c.aggregateStatus == healthpb.HealthCheckResponse_SERVING {
			healthCheckState.WithLabelValues("aggregate").Set(1)
		} else {
			healthCheckState.WithLabelValues("aggregate").Set(0)
		}
	}
}

func (c *ServerHealthChecker) logTransition(svc string, from, to healthpb.HealthCheckResponse_ServingStatus) {
	healthCheckTransitions.WithLabelValues(svc, from.String(), to.String()).Inc()
	c.lg.Info("healthcheck: state transition",
		zap.String("service", svc),
		zap.String("from", from.String()),
		zap.String("to", to.String()),
	)
}

// ── Metrics ──────────────────────────────────────────────────────────────────

var (
	metricsOnce sync.Once

	healthCheckState           *prometheus.GaugeVec
	healthCheckLastTick        prometheus.Gauge
	healthCheckTransitions     *prometheus.CounterVec
	healthCheckDuration        prometheus.Histogram
	healthCheckFailures        *prometheus.CounterVec
	healthCheckSuppressedTicks prometheus.Counter
	healthCheckApplyLag        prometheus.Gauge
	healthCheckLearnerState    prometheus.Gauge
)

func initMetricsOnce() {
	metricsOnce.Do(func() {
		healthCheckState = prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "etcd",
				Subsystem: "server",
				Name:      "healthcheck_state",
				Help:      "Serving state of the healthcheck (1=SERVING, 0=NOT_SERVING).",
			},
			[]string{"check"},
		)
		healthCheckLastTick = prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "etcd",
				Subsystem: "server",
				Name:      "healthcheck_last_tick_timestamp_seconds",
				Help:      "Timestamp of the last completed health check tick in seconds.",
			},
		)
		healthCheckTransitions = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "etcd",
				Subsystem: "server",
				Name:      "healthcheck_transitions_total",
				Help:      "Total number of serving status transitions.",
			},
			[]string{"check", "from", "to"},
		)
		healthCheckDuration = prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: "etcd",
				Subsystem: "server",
				Name:      "healthcheck_duration_seconds",
				Help:      "Histogram of health check probe pass durations.",
			},
		)
		healthCheckFailures = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "etcd",
				Subsystem: "server",
				Name:      "healthcheck_failures_total",
				Help:      "Total number of probe failures per check.",
			},
			[]string{"check"},
		)
		healthCheckSuppressedTicks = prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: "etcd",
				Subsystem: "server",
				Name:      "healthcheck_suppressed_ticks_total",
				Help:      "Total number of probe ticks suppressed due to prior probe still running.",
			},
		)
		healthCheckApplyLag = prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "etcd",
				Subsystem: "server",
				Name:      "healthcheck_apply_lag",
				Help:      "Current apply lag (committed index minus applied index).",
			},
		)
		healthCheckLearnerState = prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "etcd",
				Subsystem: "server",
				Name:      "healthcheck_learner_state",
				Help:      "Current member learner status (1=learner, 0=voter).",
			},
		)

		prometheus.MustRegister(
			healthCheckState,
			healthCheckLastTick,
			healthCheckTransitions,
			healthCheckDuration,
			healthCheckFailures,
			healthCheckSuppressedTicks,
			healthCheckApplyLag,
			healthCheckLearnerState,
		)
	})
}
