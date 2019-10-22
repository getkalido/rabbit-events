package rabbitevents

import "time"

type RabbitIni struct {
	host           string
	username       string
	password       string
	notifChannel   string
	eventChannel   string
	typingChannel  string
	connectTimeout time.Duration
}

func (m *RabbitIni) GetHost() string {
	if m != nil {
		return m.host
	}

	return ""
}

func (m *RabbitIni) GetUserName() string {
	if m != nil {
		return m.username
	}

	return ""
}

func (m *RabbitIni) GetPassword() string {
	if m != nil {
		return m.password
	}

	return ""
}

func (m *RabbitIni) GetNotifChannel() string {
	if m != nil {
		return m.notifChannel
	}

	return ""
}

func (m *RabbitIni) GetEventChannel() string {
	if m != nil {
		return m.eventChannel
	}

	return ""
}

func (m *RabbitIni) GetTypingChannel() string {
	if m != nil {
		return m.typingChannel
	}

	return ""
}

func (m *RabbitIni) GetConnectTimeout() time.Duration {
	if m != nil {
		return m.connectTimeout
	}

	return time.Second * 30
}

func NewRabbitIni(host, username, password, notifChannel, eventChannel, typingChannel string, connectTimeout time.Duration) *RabbitIni {
	return &RabbitIni{
		host:           host,
		username:       username,
		password:       password,
		notifChannel:   notifChannel,
		eventChannel:   eventChannel,
		typingChannel:  typingChannel,
		connectTimeout: connectTimeout,
	}
}
