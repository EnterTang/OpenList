package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/cmd/flags"
	"github.com/OpenListTeam/OpenList/v4/internal/cluster/transport"
	clusterworker "github.com/OpenListTeam/OpenList/v4/internal/cluster/worker"
	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/embeddedredis"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestWorkerRedisUsesEffectiveOptionsWithoutMutatingConfig(t *testing.T) {
	originalConfig := conf.Conf
	originalDataDir := flags.DataDir
	conf.Conf = conf.DefaultConfig(t.TempDir())
	flags.DataDir = t.TempDir()
	t.Cleanup(func() {
		conf.Conf = originalConfig
		flags.DataDir = originalDataDir
	})

	conf.Conf.Cluster.Role = "worker"
	conf.Conf.Cluster.Redis.Address = "127.0.0.1:16379"
	conf.Conf.Cluster.Redis.Username = "configured-user"
	conf.Conf.Cluster.Redis.Password = "configured-password"
	conf.Conf.Cluster.Redis.DB = 4
	conf.Conf.Cluster.Redis.RequireAOF = true
	before, err := json.Marshal(conf.Conf)
	require.NoError(t, err)

	manager := &embeddedredis.Manager{}
	effective := embeddedredis.EffectiveOptions{
		Address:  "127.0.0.1:26379",
		Username: "effective-user",
		Password: "effective-password",
		DB:       9,
	}
	var prepared embeddedredis.Options
	events := make([]string, 0, 2)
	runtime := &Runtime{
		role: RoleWorker,
		ctx:  context.Background(),
		prepareEmbeddedRedis: func(_ context.Context, opts embeddedredis.Options) (*embeddedredis.Manager, embeddedredis.EffectiveOptions, error) {
			events = append(events, "prepare")
			prepared = opts
			return manager, effective, nil
		},
		newRedisClient: func(opts *redis.Options) *redis.Client {
			events = append(events, "new-client")
			return redis.NewClient(opts)
		},
	}

	require.NoError(t, runtime.prepareWorkerRedisLocked())
	t.Cleanup(func() { _ = runtime.redisClient.Close() })

	require.Equal(t, []string{"prepare", "new-client"}, events)
	require.Equal(t, embeddedredis.Options{
		Role:       "worker",
		Address:    "127.0.0.1:16379",
		Username:   "configured-user",
		Password:   "configured-password",
		DB:         4,
		DataDir:    flags.DataDir,
		RequireAOF: true,
	}, prepared)
	require.Same(t, manager, runtime.embeddedRedis)
	require.Equal(t, effective.Address, runtime.redisClient.Options().Addr)
	require.Equal(t, effective.Username, runtime.redisClient.Options().Username)
	require.Equal(t, effective.Password, runtime.redisClient.Options().Password)
	require.Equal(t, effective.DB, runtime.redisClient.Options().DB)

	after, err := json.Marshal(conf.Conf)
	require.NoError(t, err)
	require.JSONEq(t, string(before), string(after))
	require.Equal(t, before, after)
}

func TestWorkerRedisPreparationErrorPreservesManagerWhenCleanupFails(t *testing.T) {
	originalConfig := conf.Conf
	conf.Conf = conf.DefaultConfig(t.TempDir())
	t.Cleanup(func() { conf.Conf = originalConfig })

	prepareErr := errors.New("prepare failed")
	stopErr := errors.New("stop failed")
	manager := &embeddedredis.Manager{}
	clientCreations := 0
	stopCalls := 0
	runtime := &Runtime{
		role: RoleWorker,
		ctx:  context.Background(),
		prepareEmbeddedRedis: func(context.Context, embeddedredis.Options) (*embeddedredis.Manager, embeddedredis.EffectiveOptions, error) {
			return manager, embeddedredis.EffectiveOptions{}, prepareErr
		},
		newRedisClient: func(opts *redis.Options) *redis.Client {
			clientCreations++
			return redis.NewClient(opts)
		},
		stopEmbeddedRedis: func(context.Context, *embeddedredis.Manager) error {
			stopCalls++
			return stopErr
		},
	}

	err := runtime.prepareWorkerRedisLocked()
	require.ErrorIs(t, err, prepareErr)
	require.ErrorIs(t, err, stopErr)
	require.Zero(t, clientCreations)
	require.Equal(t, 1, stopCalls)
	require.Nil(t, runtime.redisClient)
	require.Same(t, manager, runtime.embeddedRedis)
}

func TestWorkerRedisPreparationErrorClearsManagerAfterCleanup(t *testing.T) {
	originalConfig := conf.Conf
	conf.Conf = conf.DefaultConfig(t.TempDir())
	t.Cleanup(func() { conf.Conf = originalConfig })

	prepareErr := errors.New("prepare failed")
	manager := &embeddedredis.Manager{}
	stopCalls := 0
	runtime := &Runtime{
		role: RoleWorker,
		ctx:  context.Background(),
		prepareEmbeddedRedis: func(context.Context, embeddedredis.Options) (*embeddedredis.Manager, embeddedredis.EffectiveOptions, error) {
			return manager, embeddedredis.EffectiveOptions{}, prepareErr
		},
		newRedisClient: func(opts *redis.Options) *redis.Client {
			t.Fatal("Redis client must not be created after preparation failure")
			return redis.NewClient(opts)
		},
		stopEmbeddedRedis: func(context.Context, *embeddedredis.Manager) error {
			stopCalls++
			return nil
		},
	}

	err := runtime.prepareWorkerRedisLocked()
	require.ErrorIs(t, err, prepareErr)
	require.Equal(t, 1, stopCalls)
	require.Nil(t, runtime.redisClient)
	require.Nil(t, runtime.embeddedRedis)
}

func TestEmbeddedRedisCleanupClosesClientBeforeStoppingManager(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	manager := &embeddedredis.Manager{}
	events := make([]string, 0, 2)
	runtime := &Runtime{
		redisClient:   client,
		embeddedRedis: manager,
		closeRedisClient: func(got *redis.Client) error {
			require.Same(t, client, got)
			events = append(events, "close-client")
			return nil
		},
		stopEmbeddedRedis: func(ctx context.Context, got *embeddedredis.Manager) error {
			require.Same(t, manager, got)
			_, hasDeadline := ctx.Deadline()
			require.True(t, hasDeadline)
			events = append(events, "stop-manager")
			return nil
		},
	}

	require.NoError(t, runtime.cleanupWorkerRedisLocked())

	require.Equal(t, []string{"close-client", "stop-manager"}, events)
	require.Nil(t, runtime.redisClient)
	require.Nil(t, runtime.embeddedRedis)

	require.NoError(t, runtime.cleanupWorkerRedisLocked())
	require.Equal(t, []string{"close-client", "stop-manager"}, events)
}

func TestEmbeddedRedisCleanupFailureKeepsManagerForRetry(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	manager := &embeddedredis.Manager{}
	stopErr := errors.New("stop failed")
	events := make([]string, 0, 3)
	stopCalls := 0
	runtime := &Runtime{
		redisClient:   client,
		embeddedRedis: manager,
		closeRedisClient: func(got *redis.Client) error {
			require.Same(t, client, got)
			events = append(events, "close-client")
			return got.Close()
		},
		stopEmbeddedRedis: func(_ context.Context, got *embeddedredis.Manager) error {
			require.Same(t, manager, got)
			stopCalls++
			events = append(events, "stop-manager")
			if stopCalls == 1 {
				return stopErr
			}
			return nil
		},
	}

	err := runtime.cleanupWorkerRedisLocked()
	require.ErrorIs(t, err, stopErr)
	require.Nil(t, runtime.redisClient)
	require.Same(t, manager, runtime.embeddedRedis)

	require.NoError(t, runtime.cleanupWorkerRedisLocked())
	require.Nil(t, runtime.redisClient)
	require.Nil(t, runtime.embeddedRedis)
	require.Equal(t, []string{"close-client", "stop-manager", "stop-manager"}, events)
}

func TestWorkerRedisPreparationRefusesToOverwritePendingManager(t *testing.T) {
	originalConfig := conf.Conf
	conf.Conf = conf.DefaultConfig(t.TempDir())
	t.Cleanup(func() { conf.Conf = originalConfig })

	manager := &embeddedredis.Manager{}
	prepareCalls := 0
	clientCreations := 0
	runtime := &Runtime{
		role:          RoleWorker,
		ctx:           context.Background(),
		embeddedRedis: manager,
		prepareEmbeddedRedis: func(context.Context, embeddedredis.Options) (*embeddedredis.Manager, embeddedredis.EffectiveOptions, error) {
			prepareCalls++
			return nil, embeddedredis.EffectiveOptions{}, nil
		},
		newRedisClient: func(opts *redis.Options) *redis.Client {
			clientCreations++
			return redis.NewClient(opts)
		},
	}

	err := runtime.prepareWorkerRedisLocked()
	require.ErrorContains(t, err, "cleanup is pending")
	require.Zero(t, prepareCalls)
	require.Zero(t, clientCreations)
	require.Nil(t, runtime.redisClient)
	require.Same(t, manager, runtime.embeddedRedis)
}

func TestEmbeddedRedisManagerIsStoppedWhenLaterWorkerStartupFails(t *testing.T) {
	originalConfig := conf.Conf
	conf.Conf = conf.DefaultConfig(t.TempDir())
	t.Cleanup(func() { conf.Conf = originalConfig })

	manager := &embeddedredis.Manager{}
	stopped := false
	runtime := &Runtime{
		role: RoleWorker,
		ctx:  context.Background(),
		prepareEmbeddedRedis: func(context.Context, embeddedredis.Options) (*embeddedredis.Manager, embeddedredis.EffectiveOptions, error) {
			return manager, embeddedredis.EffectiveOptions{Address: "127.0.0.1:1"}, nil
		},
		newRedisClient: redis.NewClient,
		closeRedisClient: func(client *redis.Client) error {
			return client.Close()
		},
		stopEmbeddedRedis: func(_ context.Context, got *embeddedredis.Manager) error {
			require.Same(t, manager, got)
			stopped = true
			return nil
		},
	}

	require.NoError(t, runtime.prepareWorkerRedisLocked())
	require.Same(t, manager, runtime.embeddedRedis)
	runtime.workerClient = &transport.WorkerClient{}
	runtime.workerService = &clusterworker.Service{}

	runtime.stopLocked()

	require.True(t, stopped)
	require.Nil(t, runtime.redisClient)
	require.Nil(t, runtime.embeddedRedis)
	require.Nil(t, runtime.workerClient)
	require.Nil(t, runtime.workerService)
}

func TestWorkerRedisStartFailureCleansPreparedManagerInOrder(t *testing.T) {
	originalConfig := conf.Conf
	originalDataDir := flags.DataDir
	dataDir := t.TempDir()
	conf.Conf = conf.DefaultConfig(dataDir)
	flags.DataDir = dataDir
	t.Cleanup(func() {
		conf.Conf = originalConfig
		flags.DataDir = originalDataDir
	})

	conf.Conf.Cluster.Role = "worker"
	conf.Conf.Cluster.NodeID = "worker-start-failure"
	conf.Conf.Cluster.CoordinatorURL = "http://127.0.0.1:5244"
	conf.Conf.Cluster.WorkerKeyFile = filepath.Join(dataDir, "worker.x25519.key")

	manager := &embeddedredis.Manager{}
	workerClientErr := errors.New("create worker client")
	events := make([]string, 0, 5)
	runtime := &Runtime{
		prepareEmbeddedRedis: func(context.Context, embeddedredis.Options) (*embeddedredis.Manager, embeddedredis.EffectiveOptions, error) {
			events = append(events, "prepare")
			return manager, embeddedredis.EffectiveOptions{Address: "127.0.0.1:1"}, nil
		},
		newRedisClient: func(opts *redis.Options) *redis.Client {
			events = append(events, "new-redis-client")
			return redis.NewClient(opts)
		},
		newWorkerClient: func(transport.WorkerClientOptions) (*transport.WorkerClient, error) {
			events = append(events, "new-worker-client")
			return nil, workerClientErr
		},
		closeRedisClient: func(client *redis.Client) error {
			events = append(events, "close-client")
			return client.Close()
		},
		stopEmbeddedRedis: func(_ context.Context, got *embeddedredis.Manager) error {
			require.Same(t, manager, got)
			events = append(events, "stop-manager")
			return nil
		},
	}

	err := runtime.Start()

	require.ErrorIs(t, err, workerClientErr)
	require.Equal(t, []string{"prepare", "new-redis-client", "new-worker-client", "close-client", "stop-manager"}, events)
	require.Nil(t, runtime.redisClient)
	require.Nil(t, runtime.embeddedRedis)
	require.Nil(t, runtime.workerClient)
	require.Nil(t, runtime.workerService)
	require.False(t, runtime.started)
}

func TestEmbeddedRedisFenceCleanupClosesClientBeforeStoppingManager(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	manager := &embeddedredis.Manager{}
	events := make([]string, 0, 2)
	runtime := &Runtime{
		started:       true,
		workerClient:  &transport.WorkerClient{},
		workerService: &clusterworker.Service{},
		redisClient:   client,
		embeddedRedis: manager,
		closeRedisClient: func(got *redis.Client) error {
			require.Same(t, client, got)
			events = append(events, "close-client")
			return got.Close()
		},
		stopEmbeddedRedis: func(_ context.Context, got *embeddedredis.Manager) error {
			require.Same(t, manager, got)
			events = append(events, "stop-manager")
			return nil
		},
	}

	runtime.fenceLostCoordinator()

	require.Equal(t, []string{"close-client", "stop-manager"}, events)
	require.Nil(t, runtime.redisClient)
	require.Nil(t, runtime.embeddedRedis)
	require.Nil(t, runtime.workerClient)
	require.Nil(t, runtime.workerService)
	require.False(t, runtime.started)
}

func TestWorkerRedisBackgroundLoopsReturnWithCapturedCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	workerService := &clusterworker.Service{}
	runtime := &Runtime{}

	runtime.runReporter(ctx, workerService, nil)
	runtime.runCleanupProcessor(ctx, workerService)
}
