// Package pubsub is a Google Cloud Pub/Sub broker for forgo's worker servers. Its value is the
// worker.IBroker that worker.ConsumerConfig and worker.ProducerConfig resolve.
//
// New creates the topics and subscriptions it is given that do not exist yet, and checks the settings
// of those that do, so a topology error stops the application before it serves. Every publish waits
// until Pub/Sub has stored the message, so the outbox marks a message processed only then. Retried
// messages follow the retry policy of their subscription, and rejected ones go to its dead-letter topic
// when it has one.
package pubsub
