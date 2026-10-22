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

// Package healthcheck_test contains tests for the ServerHealthChecker.
//
// These tests are written to catch the known critical bugs identified during
// design review, in addition to covering normal functionality.
//
// BUG REGRESSION TESTS:
//   - TestApplyStall_IdleClusterNotFalsePositive (BUG 4: stall vs idle)
//   - TestDefragRefCount_ConcurrentDefrags (BUG from original health.go)
//   - TestGateInteraction_StopGRPCServiceOnDefragAndServerHealthCheck (BUG 5)
//   - TestMultiListener_AllHealthServersUpdated (BUG 2)
//   - TestCheckerLifecycle_StopsOnServerStop (BUG 3)
//   - TestMetrics_NoPanicOnMultipleServers (BUG 8)
package healthcheck_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/server/v3/etcdserver/healthcheck"
)

// ── Mock server state ────────────────────────────────────────────────────────

// fakeServerState is the mock implementation of healthcheck.ServerStateReader.
// Every field is controlled by the test — zero values produce a healthy server.
type fakeServerState struct {
	mu sync.Mutex

	lead         uint64
	memberID     uint64
	committedIdx uint64
	appliedIdx   uint64

	linearReadErr error
	serReadErr    error
	isLearner     bool
	alarms        []*pb.AlarmMember
	defragging    bool // NOTE: only used for gate-conflict test; real path uses refcount
}

func (f *fakeServerState) Lead() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lead
}

func (f *fakeServerState) MemberID() types.ID {
	f.mu.Lock()
	defer f.mu.Unlock()
	return types.ID(f.memberID)
}

func (f *fakeServerState) CommittedIndex() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.committedIdx
}

func (f *fakeServerState) AppliedIndex() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.appliedIdx
}

func (f *fakeServerState) LinearizableReadNotify(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.linearReadErr
}

func (f *fakeServerState) SerializableReadCheck(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.serReadErr
}

func (f *fakeServerState) IsLearner() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.isLearner
}

func (f *fakeServerState) Alarms() []*pb.AlarmMember {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*pb.AlarmMember, len(f.alarms))
	copy(out, f.alarms)
	return out
}

func (f *fakeServerState) ElectionTimeout() time.Duration { return 1 * time.Second }

// ── Helper constructors ───────────────────────────────────────────────────────

func healthyFakeServer(memberID uint64) *fakeServerState {
	return &fakeServerState{
		lead:         1, // has a leader
		memberID:     memberID,
		committedIdx: 100,
		appliedIdx:   100,
	}
}

func newChecker(t *testing.T, srv *fakeServerState, opts healthcheck.Options) (*healthcheck.ServerHealthChecker, *health.Server) {
	t.Helper()
	hsrv := health.NewServer()
	cfg := healthcheck.Config{
		Interval:          10 * time.Millisecond,
		Timeout:           50 * time.Millisecond,
		FailureThreshold:  3,
		SuccessThreshold:  2,
		ApplyLagThreshold: 5000,
	}
	checker := healthcheck.NewServerHealthChecker(srv, cfg, opts)
	checker.RegisterHealthServer(hsrv)
	return checker, hsrv
}

func expectStatus(t *testing.T, hsrv *health.Server, svc string, want healthpb.HealthCheckResponse_ServingStatus) {
	t.Helper()
	resp, err := hsrv.Check(context.Background(), &healthpb.HealthCheckRequest{Service: svc})
	require.NoError(t, err)
	assert.Equal(t, want, resp.Status, "health status for service %q", svc)
}

// ── Section 1: Individual check unit tests ────────────────────────────────────

func TestCheck_LinearizableRead_Healthy(t *testing.T) {
	srv := healthyFakeServer(1)
	checker, hsrv := newChecker(t, srv, healthcheck.Options{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go checker.Run(ctx)

	time.Sleep(50 * time.Millisecond)
	expectStatus(t, hsrv, healthcheck.CheckLinearRead, healthpb.HealthCheckResponse_SERVING)
	expectStatus(t, hsrv, "", healthpb.HealthCheckResponse_SERVING)
}

func TestCheck_LinearizableRead_Fails_CausesAggregateDegradation(t *testing.T) {
	srv := healthyFakeServer(1)
	srv.linearReadErr = errors.New("no quorum")
	checker, hsrv := newChecker(t, srv, healthcheck.Options{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go checker.Run(ctx)

	// Must see 3 consecutive failures (failureThreshold) before NOT_SERVING.
	// With 10ms interval: wait > 30ms + buffer.
	time.Sleep(100 * time.Millisecond)
	expectStatus(t, hsrv, healthcheck.CheckLinearRead, healthpb.HealthCheckResponse_NOT_SERVING)
	// Leader exists (srv.lead=1) → local problem → aggregate must be NOT_SERVING.
	expectStatus(t, hsrv, "", healthpb.HealthCheckResponse_NOT_SERVING)
}

func TestCheck_SerializableRead_Healthy(t *testing.T) {
	srv := healthyFakeServer(1)
	checker, hsrv := newChecker(t, srv, healthcheck.Options{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go checker.Run(ctx)
	time.Sleep(50 * time.Millisecond)
	expectStatus(t, hsrv, healthcheck.CheckSerializableRead, healthpb.HealthCheckResponse_SERVING)
}

func TestCheck_SerializableRead_FailsAlone_DoesNotDegradeAggregate(t *testing.T) {
	// Serializable read fails but linearizable read succeeds.
	// Aggregate should stay SERVING because the cluster is functioning.
	srv := healthyFakeServer(1)
	srv.serReadErr = errors.New("backend read error")
	checker, hsrv := newChecker(t, srv, healthcheck.Options{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go checker.Run(ctx)
	time.Sleep(100 * time.Millisecond)

	expectStatus(t, hsrv, healthcheck.CheckSerializableRead, healthpb.HealthCheckResponse_NOT_SERVING)
	// Aggregate stays SERVING — serializable read failure alone is not member-local fatal.
	expectStatus(t, hsrv, "", healthpb.HealthCheckResponse_SERVING)
}

func TestCheck_Learner_CausesAggregateNotServing(t *testing.T) {
	srv := healthyFakeServer(1)
	srv.isLearner = true
	checker, hsrv := newChecker(t, srv, healthcheck.Options{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go checker.Run(ctx)
	time.Sleep(100 * time.Millisecond)

	expectStatus(t, hsrv, healthcheck.CheckLearner, healthpb.HealthCheckResponse_NOT_SERVING)
	expectStatus(t, hsrv, "", healthpb.HealthCheckResponse_NOT_SERVING)
}

func TestCheck_DataCorruption_LocalMember_CausesAggregateNotServing(t *testing.T) {
	srv := healthyFakeServer(1)
	srv.alarms = []*pb.AlarmMember{
		{MemberID: 1, Alarm: pb.AlarmType_CORRUPT},
	}
	checker, hsrv := newChecker(t, srv, healthcheck.Options{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go checker.Run(ctx)
	time.Sleep(100 * time.Millisecond)

	expectStatus(t, hsrv, healthcheck.CheckDataCorruption, healthpb.HealthCheckResponse_NOT_SERVING)
	expectStatus(t, hsrv, "", healthpb.HealthCheckResponse_NOT_SERVING)
}

func TestCheck_DataCorruption_OtherMember_DoesNotDegradeLocal(t *testing.T) {
	// Alarm is on MemberID=2, but local member is 1.
	srv := healthyFakeServer(1) // memberID=1
	srv.alarms = []*pb.AlarmMember{
		{MemberID: 2, Alarm: pb.AlarmType_CORRUPT}, // different member
	}
	checker, hsrv := newChecker(t, srv, healthcheck.Options{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go checker.Run(ctx)
	time.Sleep(100 * time.Millisecond)

	// Local check should be SERVING; only remote member is corrupt.
	expectStatus(t, hsrv, healthcheck.CheckDataCorruption, healthpb.HealthCheckResponse_SERVING)
	expectStatus(t, hsrv, "", healthpb.HealthCheckResponse_SERVING)
}

func TestCheck_DataCorruption_BroadcastAlarm_MemberID0_DegradeAll(t *testing.T) {
	// MemberID=0 means cluster-wide corrupt alarm.
	srv := healthyFakeServer(1)
	srv.alarms = []*pb.AlarmMember{
		{MemberID: 0, Alarm: pb.AlarmType_CORRUPT},
	}
	checker, hsrv := newChecker(t, srv, healthcheck.Options{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go checker.Run(ctx)
	time.Sleep(100 * time.Millisecond)

	expectStatus(t, hsrv, healthcheck.CheckDataCorruption, healthpb.HealthCheckResponse_NOT_SERVING)
	expectStatus(t, hsrv, "", healthpb.HealthCheckResponse_NOT_SERVING)
}

func TestCheck_NoLeader_AloneDoesNotDegradeAggregate(t *testing.T) {
	// no_leader must NOT independently make aggregate NOT_SERVING.
	// It should only influence linear_read grace period.
	srv := healthyFakeServer(1)
	srv.lead = 0 // raft.None
	srv.linearReadErr = errors.New("no leader")
	checker, hsrv := newChecker(t, srv, healthcheck.Options{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go checker.Run(ctx)
	time.Sleep(100 * time.Millisecond)

	expectStatus(t, hsrv, healthcheck.CheckNoLeader, healthpb.HealthCheckResponse_NOT_SERVING)
	// Aggregate stays SERVING during grace period (gracePeriod = max(30s, 10*1s) = 30s).
	// After 100ms, well within grace period.
	expectStatus(t, hsrv, "", healthpb.HealthCheckResponse_SERVING)
}

func TestCheck_ApplyLag_BelowThreshold_DoesNotDegrade(t *testing.T) {
	srv := healthyFakeServer(1)
	srv.committedIdx = 100
	srv.appliedIdx = 97 // lag of 3, threshold is 5000
	checker, hsrv := newChecker(t, srv, healthcheck.Options{
		OnProbeStart: func() {
			srv.mu.Lock()
			srv.appliedIdx++ // actively advancing applied index: not stalled!
			srv.mu.Unlock()
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go checker.Run(ctx)
	time.Sleep(50 * time.Millisecond)

	expectStatus(t, hsrv, healthcheck.CheckApplyLag, healthpb.HealthCheckResponse_SERVING)
	expectStatus(t, hsrv, "", healthpb.HealthCheckResponse_SERVING)
}

func TestCheck_ApplyLag_AboveThreshold_OnlyIndividualDegrades(t *testing.T) {
	// BUG PREVENTION: apply_lag must NOT degrade aggregate.
	// High lag is cluster-wide during write bursts; marking aggregate NOT_SERVING
	// would self-inflict a cluster-wide outage.
	srv := healthyFakeServer(1)
	srv.committedIdx = 10000
	srv.appliedIdx = 1 // lag of 9999, well above threshold of 5000
	checker, hsrv := newChecker(t, srv, healthcheck.Options{
		OnProbeStart: func() {
			srv.mu.Lock()
			srv.appliedIdx++ // actively advancing applied index: not stalled!
			srv.mu.Unlock()
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go checker.Run(ctx)
	time.Sleep(100 * time.Millisecond)

	expectStatus(t, hsrv, healthcheck.CheckApplyLag, healthpb.HealthCheckResponse_NOT_SERVING)
	// CRITICAL: aggregate must still be SERVING despite high lag.
	expectStatus(t, hsrv, "", healthpb.HealthCheckResponse_SERVING)
}

// ── Section 2: Apply stall — BUG 4 regression ─────────────────────────────────

// TestApplyStall_IdleClusterNotFalsePositive catches BUG 4.
// An idle cluster has ci==ai, nothing advancing. This must NOT be a stall.
func TestApplyStall_IdleClusterNotFalsePositive(t *testing.T) {
	srv := healthyFakeServer(1)
	// Idle: ci == ai, both static.
	srv.committedIdx = 500
	srv.appliedIdx = 500
	checker, hsrv := newChecker(t, srv, healthcheck.Options{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go checker.Run(ctx)

	// Run many ticks — ci and ai never move. Must not declare stall.
	time.Sleep(200 * time.Millisecond)

	expectStatus(t, hsrv, healthcheck.CheckApplyStall, healthpb.HealthCheckResponse_SERVING)
	expectStatus(t, hsrv, "", healthpb.HealthCheckResponse_SERVING)
}

// TestApplyStall_CiAdvancesAiDoesNot is the canonical stall case.
// Cluster receives writes (ci advances) but apply loop is frozen (ai doesn't).
func TestApplyStall_CiAdvancesAiDoesNot(t *testing.T) {
	srv := healthyFakeServer(1)
	srv.committedIdx = 100
	srv.appliedIdx = 100
	checker, hsrv := newChecker(t, srv, healthcheck.Options{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go checker.Run(ctx)

	// Simulate ci advancing (writes committed) but ai frozen (apply loop stalled).
	go func() {
		time.Sleep(30 * time.Millisecond)
		srv.mu.Lock()
		srv.committedIdx = 200 // ci jumps
		// ai stays at 100    // ai does NOT advance
		srv.mu.Unlock()
	}()

	time.Sleep(200 * time.Millisecond)

	expectStatus(t, hsrv, healthcheck.CheckApplyStall, healthpb.HealthCheckResponse_NOT_SERVING)
	expectStatus(t, hsrv, "", healthpb.HealthCheckResponse_NOT_SERVING)
}

// TestApplyStall_CiStopsButLagRemains catches the missed case from BUG 4.
// ci stopped advancing, but ci > ai still exists from a previous write burst.
// Apply loop is truly stuck; naive check (ci > prevCi) would miss this.
func TestApplyStall_CiStopsButLagRemains(t *testing.T) {
	srv := healthyFakeServer(1)
	// Pre-existing lag: 50 entries not yet applied.
	srv.committedIdx = 150
	srv.appliedIdx = 100 // lag=50, apply loop stuck
	checker, hsrv := newChecker(t, srv, healthcheck.Options{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go checker.Run(ctx)

	// ci never advances again — write load stopped — but ai also never advances.
	// A naive check requiring ci>prevCi would MISS this stall entirely.
	time.Sleep(200 * time.Millisecond)

	expectStatus(t, hsrv, healthcheck.CheckApplyStall, healthpb.HealthCheckResponse_NOT_SERVING)
	expectStatus(t, hsrv, "", healthpb.HealthCheckResponse_NOT_SERVING)
}

// ── Section 3: Hysteresis / threshold tests ───────────────────────────────────

func TestHysteresis_TransientFailuresBelowThreshold(t *testing.T) {
	// Only 2 consecutive failures (threshold=3): must NOT flip to NOT_SERVING.
	var callCount int32
	srv := healthyFakeServer(1)
	srv.linearReadErr = errors.New("temporary")

	checker, hsrv := newChecker(t, srv, healthcheck.Options{
		OnProbeComplete: func() {
			n := atomic.AddInt32(&callCount, 1)
			if n >= 2 {
				// Recover after 2 failures.
				srv.mu.Lock()
				srv.linearReadErr = nil
				srv.mu.Unlock()
			}
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go checker.Run(ctx)

	time.Sleep(150 * time.Millisecond)

	// 2 failures then success — should never have hit NOT_SERVING.
	expectStatus(t, hsrv, healthcheck.CheckLinearRead, healthpb.HealthCheckResponse_SERVING)
	expectStatus(t, hsrv, "", healthpb.HealthCheckResponse_SERVING)
}

func TestHysteresis_RecoveryRequiresSuccessThreshold(t *testing.T) {
	// Reach NOT_SERVING, then succeed only once (threshold=2): must stay NOT_SERVING.
	// Then succeed again → must flip back to SERVING.
	var calls int32
	srv := healthyFakeServer(1)
	srv.linearReadErr = errors.New("failing")

	checker, hsrv := newChecker(t, srv, healthcheck.Options{
		OnProbeComplete: func() {
			n := atomic.AddInt32(&calls, 1)
			if n == 4 {
				// After 3 failures + transition, let exactly 1 success through.
				srv.mu.Lock()
				srv.linearReadErr = nil
				srv.mu.Unlock()
			}
			if n == 5 {
				// Then fail again before second success — should still be NOT_SERVING.
				srv.mu.Lock()
				srv.linearReadErr = errors.New("relapsed")
				srv.mu.Unlock()
			}
			if n == 8 {
				// Two clean successes.
				srv.mu.Lock()
				srv.linearReadErr = nil
				srv.mu.Unlock()
			}
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go checker.Run(ctx)

	// After 3 failures: NOT_SERVING.
	time.Sleep(50 * time.Millisecond)
	expectStatus(t, hsrv, healthcheck.CheckLinearRead, healthpb.HealthCheckResponse_NOT_SERVING)

	// After exactly 1 success then relapse: still NOT_SERVING.
	time.Sleep(30 * time.Millisecond)
	expectStatus(t, hsrv, healthcheck.CheckLinearRead, healthpb.HealthCheckResponse_NOT_SERVING)

	// After 2 clean successes: SERVING.
	time.Sleep(60 * time.Millisecond)
	expectStatus(t, hsrv, healthcheck.CheckLinearRead, healthpb.HealthCheckResponse_SERVING)
}

// ── Section 4: Leader-loss grace period ───────────────────────────────────────

func TestGracePeriod_LeaderLoss_TemporaryDoesNotDegradeAggregate(t *testing.T) {
	srv := healthyFakeServer(1)
	checker, hsrv := newChecker(t, srv, healthcheck.Options{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go checker.Run(ctx)

	// Simulate leader election: lose leader, linear reads fail.
	srv.mu.Lock()
	srv.lead = 0 // raft.None
	srv.linearReadErr = errors.New("no leader")
	srv.mu.Unlock()

	// Wait past failure threshold but well within grace period (grace=30s).
	time.Sleep(150 * time.Millisecond)

	expectStatus(t, hsrv, healthcheck.CheckNoLeader, healthpb.HealthCheckResponse_NOT_SERVING)
	expectStatus(t, hsrv, healthcheck.CheckLinearRead, healthpb.HealthCheckResponse_NOT_SERVING)
	// Aggregate must be SERVING — grace period not expired.
	expectStatus(t, hsrv, "", healthpb.HealthCheckResponse_SERVING)
}

func TestGracePeriod_LeaderLoss_GracePeriodExpires_DegradeAggregate(t *testing.T) {
	srv := healthyFakeServer(1)
	// Use a short grace period override for testing.
	// grace = max(30s, 10*electionTimeout) → electionTimeout=1ms → 10ms; max(30s,10ms)=30s
	// To make this testable, we set a test-only grace period override.
	checker, hsrv := newChecker(t, srv, healthcheck.Options{
		GracePeriodOverride: 50 * time.Millisecond, // override for test speed
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go checker.Run(ctx)

	// Lose leader immediately.
	srv.mu.Lock()
	srv.lead = 0
	srv.linearReadErr = errors.New("no leader")
	srv.mu.Unlock()

	// Wait for grace period to expire (50ms) + threshold failures.
	time.Sleep(250 * time.Millisecond)

	expectStatus(t, hsrv, "", healthpb.HealthCheckResponse_NOT_SERVING)
}

func TestGracePeriod_LeaderReturns_GracePeriodResets(t *testing.T) {
	srv := healthyFakeServer(1)
	checker, hsrv := newChecker(t, srv, healthcheck.Options{
		GracePeriodOverride: 50 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go checker.Run(ctx)

	// Lose leader briefly.
	srv.mu.Lock()
	srv.lead = 0
	srv.linearReadErr = errors.New("no leader")
	srv.mu.Unlock()
	time.Sleep(30 * time.Millisecond)

	// Leader returns before grace period expires.
	srv.mu.Lock()
	srv.lead = 1
	srv.linearReadErr = nil
	srv.mu.Unlock()
	time.Sleep(100 * time.Millisecond)

	// Aggregate should be SERVING; grace period should have reset.
	expectStatus(t, hsrv, "", healthpb.HealthCheckResponse_SERVING)
}

func TestGracePeriod_LinearReadFailsWithLeaderPresent_ImmediateDegradation(t *testing.T) {
	// Linear read fails but leader exists → NOT a leader-loss scenario.
	// Grace period does NOT apply. Must degrade after failureThreshold.
	srv := healthyFakeServer(1)
	srv.lead = 1 // leader is present
	srv.linearReadErr = errors.New("disk stall")
	checker, hsrv := newChecker(t, srv, healthcheck.Options{
		GracePeriodOverride: 30 * time.Second, // grace period very long
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go checker.Run(ctx)

	// Should degrade without waiting for grace period.
	time.Sleep(100 * time.Millisecond)
	expectStatus(t, hsrv, "", healthpb.HealthCheckResponse_NOT_SERVING)
}

// ── Section 5: Defragmentation reference counting — original bug regression ───

// TestDefragRefCount_ConcurrentDefrags catches the original bug where
// defragFinished() unconditionally called startServe() without reference counting.
// Two concurrent defrags: when defrag1 finishes, health must stay NOT_SERVING
// because defrag2 is still running.
func TestDefragRefCount_ConcurrentDefrags(t *testing.T) {
	srv := healthyFakeServer(1)
	checker, hsrv := newChecker(t, srv, healthcheck.Options{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go checker.Run(ctx)

	// Start two concurrent defrags.
	checker.DefragStarted()
	checker.DefragStarted()
	time.Sleep(30 * time.Millisecond)

	// Both defrags running: must be NOT_SERVING.
	expectStatus(t, hsrv, healthcheck.CheckDefrag, healthpb.HealthCheckResponse_NOT_SERVING)
	expectStatus(t, hsrv, "", healthpb.HealthCheckResponse_NOT_SERVING)

	// First defrag finishes: STILL NOT_SERVING (second still running).
	checker.DefragFinished()
	time.Sleep(30 * time.Millisecond)
	expectStatus(t, hsrv, healthcheck.CheckDefrag, healthpb.HealthCheckResponse_NOT_SERVING)
	expectStatus(t, hsrv, "", healthpb.HealthCheckResponse_NOT_SERVING)

	// Second defrag finishes: NOW SERVING.
	checker.DefragFinished()
	time.Sleep(30 * time.Millisecond)
	expectStatus(t, hsrv, healthcheck.CheckDefrag, healthpb.HealthCheckResponse_SERVING)
	expectStatus(t, hsrv, "", healthpb.HealthCheckResponse_SERVING)
}

func TestDefragRefCount_NegativeCountNeverGoesBelow0(t *testing.T) {
	// Guard against spurious DefragFinished calls without matching start.
	srv := healthyFakeServer(1)
	checker, _ := newChecker(t, srv, healthcheck.Options{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go checker.Run(ctx)

	// Call DefragFinished with no matching DefragStarted.
	// Must not panic and must not produce negative count.
	require.NotPanics(t, func() {
		checker.DefragFinished()
		checker.DefragFinished()
	})
	// Count must be 0, not negative.
	assert.Equal(t, int64(0), checker.ActiveDefragCount())
}

// ── Section 6: BUG 5 — StopGRPCServiceOnDefrag gate interaction ───────────────

// TestGateInteraction_StopGRPCServiceOnDefragAndServerHealthCheck verifies that
// when ServerHealthCheck is enabled, the old StopGRPCServiceOnDefrag direct
// SetServingStatus calls do NOT override the checker's state.
//
// Scenario: ServerHealthCheck marks "" as NOT_SERVING due to learner status.
// Then a defrag completes (old gate path would call startServe → SetServingStatus SERVING).
// The checker must detect this override and restore the correct state.
//
// Implementation note: when ServerHealthCheck is enabled, the old gate path
// must be disabled entirely. This test verifies the gate disablement.
func TestGateInteraction_ServerHealthCheckDisablesOldDefragPath(t *testing.T) {
	srv := healthyFakeServer(1)
	srv.isLearner = true // checker will mark "" as NOT_SERVING

	checker, hsrv := newChecker(t, srv, healthcheck.Options{
		ServerHealthCheckEnabled:    true,
		StopGRPCServiceOnDefragGate: true, // both gates enabled
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go checker.Run(ctx)

	time.Sleep(100 * time.Millisecond)
	// Checker correctly marks NOT_SERVING due to learner.
	expectStatus(t, hsrv, "", healthpb.HealthCheckResponse_NOT_SERVING)

	// Simulate what old gate path would have done (defrag finishes).
	// When ServerHealthCheck is enabled, DefragFinished must NOT call
	// hsrv.SetServingStatus("", SERVING) directly.
	checker.DefragFinished()
	time.Sleep(30 * time.Millisecond)

	// MUST still be NOT_SERVING — old path must be suppressed.
	expectStatus(t, hsrv, "", healthpb.HealthCheckResponse_NOT_SERVING)
}

// ── Section 7: BUG 2 — Multiple health server instances (dual listener) ────────

// TestMultiListener_AllHealthServersUpdated verifies that when two health.Server
// instances are registered (insecure + TLS listeners), both are updated.
// BUG: original plan only updated one health server.
func TestMultiListener_AllHealthServersUpdated(t *testing.T) {
	srv := healthyFakeServer(1)
	cfg := healthcheck.Config{
		Interval:          10 * time.Millisecond,
		Timeout:           50 * time.Millisecond,
		FailureThreshold:  3,
		SuccessThreshold:  2,
		ApplyLagThreshold: 5000,
	}
	checker := healthcheck.NewServerHealthChecker(srv, cfg, healthcheck.Options{})

	// Register TWO health servers (insecure + TLS listener simulation).
	hsrv1 := health.NewServer()
	hsrv2 := health.NewServer()
	checker.RegisterHealthServer(hsrv1)
	checker.RegisterHealthServer(hsrv2)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go checker.Run(ctx)

	// Both should be SERVING initially.
	time.Sleep(50 * time.Millisecond)
	expectStatus(t, hsrv1, "", healthpb.HealthCheckResponse_SERVING)
	expectStatus(t, hsrv2, "", healthpb.HealthCheckResponse_SERVING)

	// Trigger NOT_SERVING on one check.
	srv.mu.Lock()
	srv.isLearner = true
	srv.mu.Unlock()

	time.Sleep(100 * time.Millisecond)

	// BOTH health servers must be updated, not just one.
	expectStatus(t, hsrv1, "", healthpb.HealthCheckResponse_NOT_SERVING)
	expectStatus(t, hsrv2, "", healthpb.HealthCheckResponse_NOT_SERVING)
}

// ── Section 8: BUG 3 — Checker lifecycle tied to server stop ──────────────────

// TestCheckerLifecycle_StopsOnServerStop verifies the checker goroutine
// exits cleanly when context is cancelled (server stop).
// Uses a tick counter to prove the loop stops advancing.
func TestCheckerLifecycle_StopsOnServerStop(t *testing.T) {
	srv := healthyFakeServer(1)
	var ticks atomic.Int64
	checker, _ := newChecker(t, srv, healthcheck.Options{
		OnProbeComplete: func() { ticks.Add(1) },
	})

	ctx, cancel := context.WithCancel(context.Background())
	go checker.Run(ctx)

	time.Sleep(100 * time.Millisecond)
	ticksBefore := ticks.Load()
	require.Greater(t, ticksBefore, int64(0), "checker should have ticked")

	// Stop the checker.
	cancel()
	time.Sleep(50 * time.Millisecond) // allow goroutine to exit

	ticksAfter := ticks.Load()
	// Allow at most 1 more tick after cancel (in-flight), but not 5+.
	assert.InDelta(t, ticksBefore, ticksAfter, 2.0,
		"checker should stop ticking after context cancel")
}

// TestCheckerLifecycle_ShutdownSetsNotServing verifies that when the checker
// stops (via context cancel / server shutdown), all health servers are set to
// NOT_SERVING via Shutdown(). This gives clients a proper signal.
func TestCheckerLifecycle_ShutdownSetsNotServing(t *testing.T) {
	srv := healthyFakeServer(1)
	cfg := healthcheck.Config{
		Interval:          10 * time.Millisecond,
		Timeout:           50 * time.Millisecond,
		FailureThreshold:  3,
		SuccessThreshold:  2,
		ApplyLagThreshold: 5000,
	}
	checker := healthcheck.NewServerHealthChecker(srv, cfg, healthcheck.Options{})
	hsrv := health.NewServer()
	checker.RegisterHealthServer(hsrv)

	ctx, cancel := context.WithCancel(context.Background())
	go checker.Run(ctx)
	time.Sleep(50 * time.Millisecond)
	expectStatus(t, hsrv, "", healthpb.HealthCheckResponse_SERVING)

	// Stop the server.
	cancel()
	// checker.Run must call hsrv.Shutdown() on exit.
	time.Sleep(50 * time.Millisecond)

	// After shutdown, health server enters NOT_SERVING for all services.
	// Clients watching the stream should receive a final NOT_SERVING update.
	resp, err := hsrv.Check(context.Background(), &healthpb.HealthCheckRequest{Service: ""})
	require.NoError(t, err)
	assert.Equal(t, healthpb.HealthCheckResponse_NOT_SERVING, resp.Status)
}

// ── Section 9: Tick overlap suppression ───────────────────────────────────────

// TestTickOverlap_SlowProbeSkipsTick verifies that if a probe takes longer
// than the interval, the checker does NOT launch overlapping goroutines.
// This prevents runaway goroutine accumulation and recursive probe load.
func TestTickOverlap_SlowProbeSkipsTick(t *testing.T) {
	var concurrentProbes atomic.Int64
	var maxConcurrent atomic.Int64

	srv := &fakeServerState{
		lead:         1,
		memberID:     1,
		committedIdx: 100,
		appliedIdx:   100,
	}
	// Override linear read to simulate a slow probe.
	srv.linearReadErr = nil

	cfg := healthcheck.Config{
		Interval:          20 * time.Millisecond,
		Timeout:           200 * time.Millisecond, // slow timeout
		FailureThreshold:  3,
		SuccessThreshold:  2,
		ApplyLagThreshold: 5000,
	}

	checker := healthcheck.NewServerHealthChecker(srv, cfg, healthcheck.Options{
		OnProbeStart: func() {
			c := concurrentProbes.Add(1)
			for {
				old := maxConcurrent.Load()
				if c <= old || maxConcurrent.CompareAndSwap(old, c) {
					break
				}
			}
		},
		OnProbeComplete: func() {
			concurrentProbes.Add(-1)
		},
		SlowProbeDelay: 50 * time.Millisecond, // each probe takes 50ms; interval is 20ms
	})
	hsrv := health.NewServer()
	checker.RegisterHealthServer(hsrv)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go checker.Run(ctx)

	time.Sleep(300 * time.Millisecond)

	// Max concurrent probes must be 1. Overlapping is forbidden.
	assert.Equal(t, int64(1), maxConcurrent.Load(),
		"probes must never run concurrently (tick must be skipped when probe is slow)")
}

// ── Section 10: Metrics isolation ─────────────────────────────────────────────

// TestMetrics_NoPanicOnMultipleCheckerInstances verifies that creating multiple
// ServerHealthChecker instances (as integration tests do) does not panic due
// to duplicate Prometheus metric registration.
// All metrics must be registered at package init(), not per-instance.
func TestMetrics_NoPanicOnMultipleCheckerInstances(t *testing.T) {
	srv := healthyFakeServer(1)
	cfg := healthcheck.Config{
		Interval:          100 * time.Millisecond,
		Timeout:           50 * time.Millisecond,
		FailureThreshold:  3,
		SuccessThreshold:  2,
		ApplyLagThreshold: 5000,
	}

	require.NotPanics(t, func() {
		// Create three checkers — simulating three test servers in same process.
		c1 := healthcheck.NewServerHealthChecker(srv, cfg, healthcheck.Options{})
		c2 := healthcheck.NewServerHealthChecker(srv, cfg, healthcheck.Options{})
		c3 := healthcheck.NewServerHealthChecker(srv, cfg, healthcheck.Options{})
		_ = c1
		_ = c2
		_ = c3
	})
}

// ── Section 11: All-healthy baseline ─────────────────────────────────────────

func TestAllChecks_HealthyBaseline(t *testing.T) {
	srv := healthyFakeServer(1)
	checker, hsrv := newChecker(t, srv, healthcheck.Options{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go checker.Run(ctx)
	time.Sleep(100 * time.Millisecond)

	checks := []string{
		healthcheck.CheckLinearRead,
		healthcheck.CheckSerializableRead,
		healthcheck.CheckApplyLag,
		healthcheck.CheckApplyStall,
		healthcheck.CheckNoLeader,
		healthcheck.CheckDataCorruption,
		healthcheck.CheckLearner,
		healthcheck.CheckDefrag,
		"", // aggregate
	}
	for _, check := range checks {
		expectStatus(t, hsrv, check, healthpb.HealthCheckResponse_SERVING)
	}
}

// ── Section 12: Recovery ──────────────────────────────────────────────────────

func TestRecovery_AfterLearnerPromoted(t *testing.T) {
	srv := healthyFakeServer(1)
	srv.isLearner = true
	checker, hsrv := newChecker(t, srv, healthcheck.Options{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go checker.Run(ctx)

	time.Sleep(100 * time.Millisecond)
	expectStatus(t, hsrv, "", healthpb.HealthCheckResponse_NOT_SERVING)

	// Member gets promoted from learner to voter.
	srv.mu.Lock()
	srv.isLearner = false
	srv.mu.Unlock()

	// Must recover after 2 clean successes (successThreshold=2).
	time.Sleep(100 * time.Millisecond)
	expectStatus(t, hsrv, "", healthpb.HealthCheckResponse_SERVING)
}

func TestRecovery_AfterCorruptAlarmCleared(t *testing.T) {
	srv := healthyFakeServer(1)
	srv.alarms = []*pb.AlarmMember{
		{MemberID: 1, Alarm: pb.AlarmType_CORRUPT},
	}
	checker, hsrv := newChecker(t, srv, healthcheck.Options{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go checker.Run(ctx)

	time.Sleep(100 * time.Millisecond)
	expectStatus(t, hsrv, "", healthpb.HealthCheckResponse_NOT_SERVING)

	// Alarm cleared (e.g., after etcdctl alarm disarm).
	srv.mu.Lock()
	srv.alarms = nil
	srv.mu.Unlock()

	time.Sleep(100 * time.Millisecond)
	expectStatus(t, hsrv, "", healthpb.HealthCheckResponse_SERVING)
}

// ── Section 13: Edge cases ────────────────────────────────────────────────────

func TestChecker_ZeroApplyLag_Healthy(t *testing.T) {
	srv := healthyFakeServer(1)
	srv.committedIdx = 100
	srv.appliedIdx = 100 // exactly equal, no lag
	checker, hsrv := newChecker(t, srv, healthcheck.Options{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go checker.Run(ctx)
	time.Sleep(50 * time.Millisecond)
	expectStatus(t, hsrv, healthcheck.CheckApplyLag, healthpb.HealthCheckResponse_SERVING)
	expectStatus(t, hsrv, healthcheck.CheckApplyStall, healthpb.HealthCheckResponse_SERVING)
}

func TestChecker_DefragImmediateStatusUpdate(t *testing.T) {
	// When DefragStarted() is called, the status update must happen
	// immediately (not waiting for the next 500ms tick).
	srv := healthyFakeServer(1)
	cfg := healthcheck.Config{
		Interval:          5 * time.Second, // very long interval
		Timeout:           50 * time.Millisecond,
		FailureThreshold:  3,
		SuccessThreshold:  2,
		ApplyLagThreshold: 5000,
	}
	checker := healthcheck.NewServerHealthChecker(srv, cfg, healthcheck.Options{})
	hsrv := health.NewServer()
	checker.RegisterHealthServer(hsrv)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go checker.Run(ctx)

	// Trigger defrag — should update status immediately, not after 5s.
	checker.DefragStarted()
	time.Sleep(30 * time.Millisecond) // much less than 5s interval

	expectStatus(t, hsrv, healthcheck.CheckDefrag, healthpb.HealthCheckResponse_NOT_SERVING)
	expectStatus(t, hsrv, "", healthpb.HealthCheckResponse_NOT_SERVING)
}

func TestChecker_MultipleAlarms_OnlyCorruptMatters(t *testing.T) {
	// NOSPACE alarm on local member should NOT trigger data_corruption check.
	srv := healthyFakeServer(1)
	srv.alarms = []*pb.AlarmMember{
		{MemberID: 1, Alarm: pb.AlarmType_NOSPACE},
	}
	checker, hsrv := newChecker(t, srv, healthcheck.Options{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go checker.Run(ctx)
	time.Sleep(100 * time.Millisecond)

	// NOSPACE is not a data corruption issue.
	expectStatus(t, hsrv, healthcheck.CheckDataCorruption, healthpb.HealthCheckResponse_SERVING)
}

// ── Section 14: Race detector validation ─────────────────────────────────────

// TestRaceDetector_ConcurrentStateChanges runs the checker while concurrently
// mutating all server state fields. Run with -race to detect data races.
func TestRaceDetector_ConcurrentStateChanges(t *testing.T) {
	srv := healthyFakeServer(1)
	checker, _ := newChecker(t, srv, healthcheck.Options{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go checker.Run(ctx)

	var wg sync.WaitGroup

	// Goroutine 1: Flip leader.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			srv.mu.Lock()
			if srv.lead == 0 {
				srv.lead = 1
			} else {
				srv.lead = 0
			}
			srv.mu.Unlock()
			time.Sleep(2 * time.Millisecond)
		}
	}()

	// Goroutine 2: Advance indices.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			srv.mu.Lock()
			srv.committedIdx += 10
			srv.appliedIdx += 9
			srv.mu.Unlock()
			time.Sleep(2 * time.Millisecond)
		}
	}()

	// Goroutine 3: Flip learner.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			srv.mu.Lock()
			srv.isLearner = !srv.isLearner
			srv.mu.Unlock()
			time.Sleep(3 * time.Millisecond)
		}
	}()

	// Goroutine 4: Concurrent defrags.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			checker.DefragStarted()
			time.Sleep(5 * time.Millisecond)
			checker.DefragFinished()
			time.Sleep(5 * time.Millisecond)
		}
	}()

	wg.Wait()
	// No race, no panic, no deadlock — that's the assertion.
}
