// Package rabbitmq is a RabbitMQ broker for forgo's worker servers. Its value is the worker.IBroker that
// worker.ConsumerConfig and worker.ProducerConfig resolve.
//
// New connects and declares the whole topology: exchanges, queues, bindings, and the dead-letter and
// retry queues of the queues that enable them, so a topology error stops the application before it
// serves. Every publisher waits for the broker's confirm, so the outbox marks a message processed only
// once the broker is responsible for it. A lost connection or channel is recovered, and the late
// acknowledgements of messages delivered before the loss are dropped instead of reaching the recovered
// channel.
//
// See the [rabbitmq] section of REFERENCE.md.
//
// [rabbitmq]: https://github.com/efegreat/forgo-adapter/blob/main/REFERENCE.md#rabbitmq
package rabbitmq
