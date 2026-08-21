package service

import (
	agentledger "github.com/compforge/agent-ledger/go"
	managedevent "github.com/compforge/agentd/internal/event"
)

type EventLog = managedevent.Log

func NewEventLog(store agentledger.EventStore) *EventLog {
	return managedevent.NewLog(store)
}

func NewManagedEvent(eventType string, fields map[string]any) ManagedEvent {
	return managedevent.New(eventType, fields)
}

func NewTurnEvent(inputID, eventType string, fields map[string]any) ManagedEvent {
	return managedevent.NewTurn(inputID, eventType, fields)
}
