package biz

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// resetRealtimeModelSyncState 把包级状态恢复成进程刚启动的样子，并在用例结束后还原。
func resetRealtimeModelSyncState(t *testing.T, ttl time.Duration) {
	t.Helper()

	oldTTL := realtimeModelSyncTTL

	realtimeModelSyncMu.Lock()
	oldAt, oldRunning := realtimeModelSyncedAt, realtimeModelSyncRunning
	realtimeModelSyncedAt, realtimeModelSyncRunning = time.Time{}, false
	realtimeModelSyncMu.Unlock()

	realtimeModelSyncTTL = ttl

	t.Cleanup(func() {
		realtimeModelSyncTTL = oldTTL

		realtimeModelSyncMu.Lock()
		realtimeModelSyncedAt, realtimeModelSyncRunning = oldAt, oldRunning
		realtimeModelSyncMu.Unlock()
	})
}

func TestRealtimeModelSync_DisabledWithoutTTL(t *testing.T) {
	resetRealtimeModelSyncState(t, 0)
	require.False(t, RealtimeModelSyncEnabled())
}

func TestRealtimeModelSync_FirstCallStarts(t *testing.T) {
	resetRealtimeModelSyncState(t, 5*time.Minute)
	require.True(t, RealtimeModelSyncEnabled())
	require.True(t, beginRealtimeModelSync(time.Now()), "首次调用应该抢到闸门")
}

func TestRealtimeModelSync_SingleFlightWhileRunning(t *testing.T) {
	resetRealtimeModelSyncState(t, 5*time.Minute)

	now := time.Now()
	require.True(t, beginRealtimeModelSync(now))
	// 上一轮还在跑：哪怕已经过了很久，也不开第二个 goroutine。
	require.False(t, beginRealtimeModelSync(now.Add(time.Hour)))
}

func TestRealtimeModelSync_TTLGateAfterFinish(t *testing.T) {
	resetRealtimeModelSyncState(t, 5*time.Minute)

	start := time.Now()
	require.True(t, beginRealtimeModelSync(start))

	// 这一轮跑了 2 分钟，TTL 从「结束时刻」算起。
	finishedAt := start.Add(2 * time.Minute)
	finishRealtimeModelSync(finishedAt)

	require.False(t, beginRealtimeModelSync(finishedAt.Add(4*time.Minute)), "TTL 未到不该重开")
	require.True(t, beginRealtimeModelSync(finishedAt.Add(5*time.Minute)), "TTL 到了应该重开")
}

func TestRealtimeModelSyncDurationFromEnv(t *testing.T) {
	const name = "AXONHUB_TEST_REALTIME_DURATION"

	for _, tc := range []struct {
		raw  string
		want time.Duration
	}{
		{"", 7 * time.Second},     // 未配置 → 默认值
		{"   ", 7 * time.Second},  // 只有空白 → 默认值
		{"abc", 7 * time.Second},  // 非法 → 默认值
		{"0s", 7 * time.Second},   // <= 0 → 默认值
		{"-1m", 7 * time.Second},  // 负数 → 默认值
		{"90s", 90 * time.Second}, // 合法
		{"5m", 5 * time.Minute},   // 合法
	} {
		t.Setenv(name, tc.raw)
		require.Equal(t, tc.want, realtimeModelSyncDurationFromEnv(name, 7*time.Second), "raw=%q", tc.raw)
	}
}
