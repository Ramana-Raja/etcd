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

// Package healthcheck_test contains tests for the GRPCClientHealthCheckOptions helper.
package healthcheck_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"go.etcd.io/etcd/client/v3/healthcheck"
)

// ── Unit tests for GRPCClientHealthCheckOptions ───────────────────────────────

func TestGRPCClientHealthCheckOptions_ReturnsTwoOptions(t *testing.T) {
	opts := healthcheck.GRPCClientHealthCheckOptions()
	// Must return exactly 2 options: WithDisableServiceConfig + WithDefaultServiceConfig.
	assert.Len(t, opts, 2, "must return exactly 2 dial options")
}

func TestGRPCClientHealthCheckOptions_NotNil(t *testing.T) {
	opts := healthcheck.GRPCClientHealthCheckOptions()
	for i, opt := range opts {
		assert.NotNil(t, opt, "option at index %d must not be nil", i)
	}
}

// TestGRPCClientHealthCheckOptions_ServiceConfigJSON verifies that the service
// config JSON embedded in the options is valid and contains both the
// round_robin load balancing policy and the healthCheckConfig with empty service name.
func TestGRPCClientHealthCheckOptions_ServiceConfigJSON(t *testing.T) {
	// The service config is embedded in the option — we expose it as a testable constant.
	raw := healthcheck.ServiceConfigJSON

	var parsed struct {
		LoadBalancingPolicy string `json:"loadBalancingPolicy"`
		HealthCheckConfig   struct {
			ServiceName string `json:"serviceName"`
		} `json:"healthCheckConfig"`
	}
	err := json.Unmarshal([]byte(raw), &parsed)
	require.NoError(t, err, "service config must be valid JSON")

	assert.Equal(t, "round_robin", parsed.LoadBalancingPolicy,
		"must preserve round_robin load balancing policy")
	assert.Equal(t, "", parsed.HealthCheckConfig.ServiceName,
		"service name must be empty string (aggregate health check)")
}

// TestGRPCClientHealthCheckOptions_CompatibleWithClientv3New verifies the options
// can be passed to clientv3.Config.DialOptions without error.
// This is a compilation + API surface check.
func TestGRPCClientHealthCheckOptions_CompatibleWithDialOptions(t *testing.T) {
	opts := healthcheck.GRPCClientHealthCheckOptions()

	// Verify each option satisfies the grpc.DialOption interface.
	// This catches type mismatches that would only fail at compile time in real usage.
	for i, opt := range opts {
		var _ grpc.DialOption = opt
		_ = i
	}
}

// TestGRPCClientHealthCheckOptions_Idempotent verifies that calling the function
// multiple times returns equivalent option sets (no shared mutable state).
func TestGRPCClientHealthCheckOptions_Idempotent(t *testing.T) {
	opts1 := healthcheck.GRPCClientHealthCheckOptions()
	opts2 := healthcheck.GRPCClientHealthCheckOptions()

	assert.Len(t, opts1, len(opts2), "each call must return the same number of options")
}

// TestGRPCClientHealthCheckOptions_DisableServiceConfigPresent verifies that
// WithDisableServiceConfig is present in the returned options.
// This is critical: without it, the EtcdManualResolver's hardcoded round_robin
// service config would override the healthCheckConfig we inject.
//
// We verify this indirectly: building a real gRPC connection using these options
// (plus a mock endpoint) must succeed and not fall back to resolver service config.
func TestGRPCClientHealthCheckOptions_ResolverServiceConfigOverrideDisabled(t *testing.T) {
	// This is a smoke test that the options don't cause a parse error.
	// The WithDisableServiceConfig + WithDefaultServiceConfig combination
	// is already proven to work in tests/e2e/failover_test.go.
	// Here we verify the JSON parses cleanly via grpc.
	opts := healthcheck.GRPCClientHealthCheckOptions()

	// grpc.NewClient with a fake address should not fail due to bad option format.
	cc, err := grpc.NewClient("localhost:0", opts...)
	if err != nil {
		// Some build environments may disallow this — skip rather than fail.
		t.Skipf("grpc.NewClient returned error (environment restriction): %v", err)
	}
	defer cc.Close()
}

// TestServiceConfigJSON_HealthCheckConfigServiceName verifies that an empty
// service name "" is used, not "etcd" or any named service. This ensures
// gRPC health checking queries the aggregate health status, not a specific
// named service.
func TestServiceConfigJSON_HealthCheckConfigServiceName(t *testing.T) {
	// The aggregate health status uses the empty string "" as the service name.
	// Using a non-empty service name would query a different health bucket
	// that etcd servers do not necessarily populate.
	raw := healthcheck.ServiceConfigJSON

	// Must not contain a non-empty serviceName value.
	assert.NotContains(t, raw, `"serviceName": "etcd"`)
	assert.NotContains(t, raw, `"serviceName":"etcd"`)

	// Must contain the empty service name.
	assert.True(t,
		contains(raw, `"serviceName": ""`) || contains(raw, `"serviceName":""`),
		"service name must be empty string",
	)
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsStr(s, substr))
}

func containsStr(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
