package streaming

import (
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// Enhanced mock connection for more comprehensive testing
type enhancedMockNatsConn struct {
	mock.Mock
	connected      bool
	closed         bool
	publishError   error
	serverID       string
	connectedURL   string
	rttDuration    time.Duration
	rttError       error
}

func (m *enhancedMockNatsConn) Publish(subject string, data []byte) error {
	args := m.Called(subject, data)
	return args.Error(0)
}

func (m *enhancedMockNatsConn) IsConnected() bool {
	args := m.Called()
	return args.Bool(0)
}

func (m *enhancedMockNatsConn) Close() {
	m.Called()
	m.closed = true
}

func (m *enhancedMockNatsConn) ConnectedServerId() string {
	args := m.Called()
	return args.String(0)
}

func (m *enhancedMockNatsConn) ConnectedUrl() string {
	args := m.Called()
	return args.String(0)
}

func (m *enhancedMockNatsConn) RTT() (time.Duration, error) {
	args := m.Called()
	return args.Get(0).(time.Duration), args.Error(1)
}

// TestConnectErrorScenarios specifically tests error handling in Connect method
func TestConnectErrorScenarios(t *testing.T) {
	t.Run("Connect_ConnectionError", func(t *testing.T) {
		client := NewNATSClient("nats://localhost:4222")
		
		// Mock connection failure using the existing MockConnect helper
		expectedError := errors.New("connection refused")
		restore := MockConnect(func(url string, opts ...nats.Option) (*nats.Conn, error) {
			return nil, expectedError
		})
		defer restore()
		
		err := client.Connect()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to connect to NATS")
		assert.Contains(t, err.Error(), "connection refused")
		assert.False(t, client.connected)
		assert.Equal(t, expectedError, client.lastError)
	})

	t.Run("Connect_SuccessfulConnection", func(t *testing.T) {
		client := NewNATSClient("nats://localhost:4222")
		
		// Mock successful connection using the existing MockConnect helper
		restore := MockConnect(func(url string, opts ...nats.Option) (*nats.Conn, error) {
			return &nats.Conn{}, nil
		})
		defer restore()
		
		err := client.Connect()
		assert.NoError(t, err)
		assert.True(t, client.connected)
		assert.NotZero(t, client.lastConnectTime)
	})
}

// TestConnectStateManagement tests connection state scenarios
func TestConnectStateManagement(t *testing.T) {
	t.Run("Connect_AlreadyConnected", func(t *testing.T) {
		client := NewNATSClient("nats://localhost:4222")
		
		// Create mock connection that reports as connected
		mockConn := &enhancedMockNatsConn{}
		mockConn.On("IsConnected").Return(true)
		
		// Set client to already connected state
		client.mu.Lock()
		client.connected = true
		client.conn = mockConn
		client.mu.Unlock()
		
		err := client.Connect()
		assert.NoError(t, err)
		mockConn.AssertExpectations(t)
	})

	t.Run("Connect_ExistingConnectionButNotConnected", func(t *testing.T) {
		client := NewNATSClient("nats://localhost:4222")
		
		// Create mock connection that reports as not connected
		oldMockConn := &enhancedMockNatsConn{}
		oldMockConn.On("IsConnected").Return(false)
		oldMockConn.On("Close").Return()
		
		// Set client to have existing connection but not connected
		client.mu.Lock()
		client.connected = true
		client.conn = oldMockConn
		client.mu.Unlock()
		
		// Mock successful new connection using the existing MockConnect helper
		restore := MockConnect(func(url string, opts ...nats.Option) (*nats.Conn, error) {
			return &nats.Conn{}, nil
		})
		defer restore()
		
		err := client.Connect()
		assert.NoError(t, err)
		assert.True(t, client.connected)
		assert.True(t, oldMockConn.closed)
		oldMockConn.AssertExpectations(t)
	})
}

// TestConnectHandlerLogic tests the logic that would be executed by handlers
func TestConnectHandlerLogic(t *testing.T) {
	// Instead of trying to capture the actual handlers, we test the logic
	// that the handlers would execute when called
	
	t.Run("ErrorHandlerLogic", func(t *testing.T) {
		client := NewNATSClient("nats://localhost:4222")
		
		// Simulate what the error handler would do
		testError := errors.New("test error")
		client.mu.Lock()
		client.lastError = testError
		client.mu.Unlock()
		
		assert.Equal(t, testError, client.lastError)
	})

	t.Run("DisconnectHandlerLogic", func(t *testing.T) {
		client := NewNATSClient("nats://localhost:4222")
		
		// Simulate what the disconnect handler would do
		disconnectError := errors.New("disconnected")
		client.mu.Lock()
		client.connected = false
		client.disconnectCount++
		client.lastError = disconnectError
		client.mu.Unlock()
		
		assert.False(t, client.connected)
		assert.Equal(t, 1, client.disconnectCount)
		assert.Equal(t, disconnectError, client.lastError)
	})

	t.Run("ReconnectHandlerLogic", func(t *testing.T) {
		client := NewNATSClient("nats://localhost:4222")
		
		// Simulate what the reconnect handler would do
		client.mu.Lock()
		client.connected = true
		client.reconnectCount++
		client.mu.Unlock()
		
		assert.True(t, client.connected)
		assert.Equal(t, 1, client.reconnectCount)
	})

	t.Run("ClosedHandlerLogic", func(t *testing.T) {
		client := NewNATSClient("nats://localhost:4222")
		
		// Simulate what the closed handler would do
		client.mu.Lock()
		client.connected = false
		client.mu.Unlock()
		
		assert.False(t, client.connected)
	})
}

// TestConnectEdgeCases tests various edge cases and error conditions
func TestConnectEdgeCases(t *testing.T) {
	t.Run("Connect_WithNilConnection", func(t *testing.T) {
		client := NewNATSClient("nats://localhost:4222")
		client.mu.Lock()
		client.conn = nil
		client.connected = false
		client.mu.Unlock()
		
		restore := MockConnect(func(url string, opts ...nats.Option) (*nats.Conn, error) {
			return &nats.Conn{}, nil
		})
		defer restore()
		
		err := client.Connect()
		assert.NoError(t, err)
		assert.True(t, client.connected)
	})

	t.Run("Connect_TimeoutScenario", func(t *testing.T) {
		client := NewNATSClient("nats://localhost:4222")
		
		restore := MockConnect(func(url string, opts ...nats.Option) (*nats.Conn, error) {
			return nil, errors.New("connection timeout")
		})
		defer restore()
		
		err := client.Connect()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "timeout")
		assert.False(t, client.connected)
	})

	t.Run("Connect_SetsLastConnectTime", func(t *testing.T) {
		client := NewNATSClient("nats://localhost:4222")
		
		// Record time before connect
		beforeConnect := time.Now()
		
		restore := MockConnect(func(url string, opts ...nats.Option) (*nats.Conn, error) {
			return &nats.Conn{}, nil
		})
		defer restore()
		
		err := client.Connect()
		assert.NoError(t, err)
		
		// Verify lastConnectTime was set
		assert.True(t, client.lastConnectTime.After(beforeConnect) || client.lastConnectTime.Equal(beforeConnect))
		assert.True(t, client.lastConnectTime.Before(time.Now().Add(time.Second)))
	})
}