//go:generate mockgen -package=test -destination=auto_generated_rabbitGenMock.go github.com/getkalido/rabbit-events RabbitEventHandler,EventConsumer,RabbitExchange
package test
