// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/conduitio-labs/conduit-connector-neo4j/destination (interfaces: Writer)
//
// Generated by this command:
//
//	mockgen -package mock -destination mock/destination.go . Writer
//

// Package mock is a generated GoMock package.
package mock

import (
	context "context"
	reflect "reflect"

	opencdc "github.com/conduitio/conduit-commons/opencdc"
	gomock "go.uber.org/mock/gomock"
)

// MockWriter is a mock of Writer interface.
type MockWriter struct {
	ctrl     *gomock.Controller
	recorder *MockWriterMockRecorder
}

// MockWriterMockRecorder is the mock recorder for MockWriter.
type MockWriterMockRecorder struct {
	mock *MockWriter
}

// NewMockWriter creates a new mock instance.
func NewMockWriter(ctrl *gomock.Controller) *MockWriter {
	mock := &MockWriter{ctrl: ctrl}
	mock.recorder = &MockWriterMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockWriter) EXPECT() *MockWriterMockRecorder {
	return m.recorder
}

// Write mocks base method.
func (m *MockWriter) Write(arg0 context.Context, arg1 opencdc.Record) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Write", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// Write indicates an expected call of Write.
func (mr *MockWriterMockRecorder) Write(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Write", reflect.TypeOf((*MockWriter)(nil).Write), arg0, arg1)
}
