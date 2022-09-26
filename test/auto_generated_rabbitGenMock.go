// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/getkalido/rabbit-events (interfaces: RabbitEventHandler,EventConsumer,RabbitExchange,RabbitConfig)

// Package test is a generated GoMock package.
package test

import (
	reflect "reflect"
	time "time"

	rabbitevents "github.com/getkalido/rabbit-events"
	gomock "github.com/golang/mock/gomock"
)

// MockRabbitEventHandler is a mock of RabbitEventHandler interface.
type MockRabbitEventHandler struct {
	ctrl     *gomock.Controller
	recorder *MockRabbitEventHandlerMockRecorder
}

// MockRabbitEventHandlerMockRecorder is the mock recorder for MockRabbitEventHandler.
type MockRabbitEventHandlerMockRecorder struct {
	mock *MockRabbitEventHandler
}

// NewMockRabbitEventHandler creates a new mock instance.
func NewMockRabbitEventHandler(ctrl *gomock.Controller) *MockRabbitEventHandler {
	mock := &MockRabbitEventHandler{ctrl: ctrl}
	mock.recorder = &MockRabbitEventHandlerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockRabbitEventHandler) EXPECT() *MockRabbitEventHandlerMockRecorder {
	return m.recorder
}

// Consume mocks base method.
func (m *MockRabbitEventHandler) Consume(arg0 string, arg1 ...func() interface{}) (rabbitevents.EventConsumer, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0}
	for _, a := range arg1 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "Consume", varargs...)
	ret0, _ := ret[0].(rabbitevents.EventConsumer)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Consume indicates an expected call of Consume.
func (mr *MockRabbitEventHandlerMockRecorder) Consume(arg0 interface{}, arg1 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0}, arg1...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Consume", reflect.TypeOf((*MockRabbitEventHandler)(nil).Consume), varargs...)
}

// Emit mocks base method.
func (m *MockRabbitEventHandler) Emit(arg0 string) rabbitevents.EventEmitter {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Emit", arg0)
	ret0, _ := ret[0].(rabbitevents.EventEmitter)
	return ret0
}

// Emit indicates an expected call of Emit.
func (mr *MockRabbitEventHandlerMockRecorder) Emit(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Emit", reflect.TypeOf((*MockRabbitEventHandler)(nil).Emit), arg0)
}

// MockEventConsumer is a mock of EventConsumer interface.
type MockEventConsumer struct {
	ctrl     *gomock.Controller
	recorder *MockEventConsumerMockRecorder
}

// MockEventConsumerMockRecorder is the mock recorder for MockEventConsumer.
type MockEventConsumerMockRecorder struct {
	mock *MockEventConsumer
}

// NewMockEventConsumer creates a new mock instance.
func NewMockEventConsumer(ctrl *gomock.Controller) *MockEventConsumer {
	mock := &MockEventConsumer{ctrl: ctrl}
	mock.recorder = &MockEventConsumerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockEventConsumer) EXPECT() *MockEventConsumerMockRecorder {
	return m.recorder
}

// Subscribe mocks base method.
func (m *MockEventConsumer) Subscribe(arg0 []int64, arg1 func(*rabbitevents.Event)) rabbitevents.Unsubscribe {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Subscribe", arg0, arg1)
	ret0, _ := ret[0].(rabbitevents.Unsubscribe)
	return ret0
}

// Subscribe indicates an expected call of Subscribe.
func (mr *MockEventConsumerMockRecorder) Subscribe(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Subscribe", reflect.TypeOf((*MockEventConsumer)(nil).Subscribe), arg0, arg1)
}

// SubscribeUnfiltered mocks base method.
func (m *MockEventConsumer) SubscribeUnfiltered(arg0 func(*rabbitevents.Event)) rabbitevents.Unsubscribe {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SubscribeUnfiltered", arg0)
	ret0, _ := ret[0].(rabbitevents.Unsubscribe)
	return ret0
}

// SubscribeUnfiltered indicates an expected call of SubscribeUnfiltered.
func (mr *MockEventConsumerMockRecorder) SubscribeUnfiltered(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SubscribeUnfiltered", reflect.TypeOf((*MockEventConsumer)(nil).SubscribeUnfiltered), arg0)
}

// MockRabbitExchange is a mock of RabbitExchange interface.
type MockRabbitExchange struct {
	ctrl     *gomock.Controller
	recorder *MockRabbitExchangeMockRecorder
}

// MockRabbitExchangeMockRecorder is the mock recorder for MockRabbitExchange.
type MockRabbitExchangeMockRecorder struct {
	mock *MockRabbitExchange
}

// NewMockRabbitExchange creates a new mock instance.
func NewMockRabbitExchange(ctrl *gomock.Controller) *MockRabbitExchange {
	mock := &MockRabbitExchange{ctrl: ctrl}
	mock.recorder = &MockRabbitExchangeMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockRabbitExchange) EXPECT() *MockRabbitExchangeMockRecorder {
	return m.recorder
}

// BulkReceive mocks base method.
func (m *MockRabbitExchange) BulkReceive(arg0 rabbitevents.ExchangeSettings, arg1 rabbitevents.QueueSettings, arg2 time.Duration) (func(rabbitevents.BulkMessageHandleFunc) error, func(), error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "BulkReceive", arg0, arg1, arg2)
	ret0, _ := ret[0].(func(rabbitevents.BulkMessageHandleFunc) error)
	ret1, _ := ret[1].(func())
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// BulkReceive indicates an expected call of BulkReceive.
func (mr *MockRabbitExchangeMockRecorder) BulkReceive(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "BulkReceive", reflect.TypeOf((*MockRabbitExchange)(nil).BulkReceive), arg0, arg1, arg2)
}

// BulkReceiveFrom mocks base method.
func (m *MockRabbitExchange) BulkReceiveFrom(arg0, arg1 string, arg2, arg3 bool, arg4, arg5 string, arg6 int, arg7 time.Duration) (func(rabbitevents.BulkMessageHandleFunc) error, func(), error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "BulkReceiveFrom", arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7)
	ret0, _ := ret[0].(func(rabbitevents.BulkMessageHandleFunc) error)
	ret1, _ := ret[1].(func())
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// BulkReceiveFrom indicates an expected call of BulkReceiveFrom.
func (mr *MockRabbitExchangeMockRecorder) BulkReceiveFrom(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "BulkReceiveFrom", reflect.TypeOf((*MockRabbitExchange)(nil).BulkReceiveFrom), arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7)
}

// Close mocks base method.
func (m *MockRabbitExchange) Close() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close.
func (mr *MockRabbitExchangeMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockRabbitExchange)(nil).Close))
}

// Receive mocks base method.
func (m *MockRabbitExchange) Receive(arg0 rabbitevents.ExchangeSettings, arg1 rabbitevents.QueueSettings) (func(rabbitevents.MessageHandleFunc) error, func(), error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Receive", arg0, arg1)
	ret0, _ := ret[0].(func(rabbitevents.MessageHandleFunc) error)
	ret1, _ := ret[1].(func())
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// Receive indicates an expected call of Receive.
func (mr *MockRabbitExchangeMockRecorder) Receive(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Receive", reflect.TypeOf((*MockRabbitExchange)(nil).Receive), arg0, arg1)
}

// ReceiveFrom mocks base method.
func (m *MockRabbitExchange) ReceiveFrom(arg0, arg1 string, arg2, arg3 bool, arg4, arg5 string) (func(rabbitevents.MessageHandleFunc) error, func(), error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ReceiveFrom", arg0, arg1, arg2, arg3, arg4, arg5)
	ret0, _ := ret[0].(func(rabbitevents.MessageHandleFunc) error)
	ret1, _ := ret[1].(func())
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// ReceiveFrom indicates an expected call of ReceiveFrom.
func (mr *MockRabbitExchangeMockRecorder) ReceiveFrom(arg0, arg1, arg2, arg3, arg4, arg5 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReceiveFrom", reflect.TypeOf((*MockRabbitExchange)(nil).ReceiveFrom), arg0, arg1, arg2, arg3, arg4, arg5)
}

// SendTo mocks base method.
func (m *MockRabbitExchange) SendTo(arg0, arg1 string, arg2, arg3 bool, arg4 string) rabbitevents.MessageHandleFunc {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SendTo", arg0, arg1, arg2, arg3, arg4)
	ret0, _ := ret[0].(rabbitevents.MessageHandleFunc)
	return ret0
}

// SendTo indicates an expected call of SendTo.
func (mr *MockRabbitExchangeMockRecorder) SendTo(arg0, arg1, arg2, arg3, arg4 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SendTo", reflect.TypeOf((*MockRabbitExchange)(nil).SendTo), arg0, arg1, arg2, arg3, arg4)
}

// MockRabbitConfig is a mock of RabbitConfig interface.
type MockRabbitConfig struct {
	ctrl     *gomock.Controller
	recorder *MockRabbitConfigMockRecorder
}

// MockRabbitConfigMockRecorder is the mock recorder for MockRabbitConfig.
type MockRabbitConfigMockRecorder struct {
	mock *MockRabbitConfig
}

// NewMockRabbitConfig creates a new mock instance.
func NewMockRabbitConfig(ctrl *gomock.Controller) *MockRabbitConfig {
	mock := &MockRabbitConfig{ctrl: ctrl}
	mock.recorder = &MockRabbitConfigMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockRabbitConfig) EXPECT() *MockRabbitConfigMockRecorder {
	return m.recorder
}

// GetConnectTimeout mocks base method.
func (m *MockRabbitConfig) GetConnectTimeout() time.Duration {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetConnectTimeout")
	ret0, _ := ret[0].(time.Duration)
	return ret0
}

// GetConnectTimeout indicates an expected call of GetConnectTimeout.
func (mr *MockRabbitConfigMockRecorder) GetConnectTimeout() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetConnectTimeout", reflect.TypeOf((*MockRabbitConfig)(nil).GetConnectTimeout))
}

// GetHost mocks base method.
func (m *MockRabbitConfig) GetHost() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetHost")
	ret0, _ := ret[0].(string)
	return ret0
}

// GetHost indicates an expected call of GetHost.
func (mr *MockRabbitConfigMockRecorder) GetHost() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetHost", reflect.TypeOf((*MockRabbitConfig)(nil).GetHost))
}

// GetPassword mocks base method.
func (m *MockRabbitConfig) GetPassword() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetPassword")
	ret0, _ := ret[0].(string)
	return ret0
}

// GetPassword indicates an expected call of GetPassword.
func (mr *MockRabbitConfigMockRecorder) GetPassword() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetPassword", reflect.TypeOf((*MockRabbitConfig)(nil).GetPassword))
}

// GetUserName mocks base method.
func (m *MockRabbitConfig) GetUserName() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetUserName")
	ret0, _ := ret[0].(string)
	return ret0
}

// GetUserName indicates an expected call of GetUserName.
func (mr *MockRabbitConfigMockRecorder) GetUserName() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetUserName", reflect.TypeOf((*MockRabbitConfig)(nil).GetUserName))
}
