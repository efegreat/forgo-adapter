// Package redisstream is a Redis Streams broker for forgo's worker servers. Its value is the
// worker.IBroker that worker.ConsumerConfig and worker.ProducerConfig resolve.
//
// Publishers add entries to the stream a message's Destination names, and every subscriber reads a
// stream as a consumer of its own in a consumer group. New creates the consumer groups that do not
// exist yet. The entries of a consumer that stops, such as an instance that crashed, are claimed by
// another consumer of the group once they have been idle for the subscriber's ClaimIdle. Rejected
// entries go to a dead-letter stream when the subscriber enables one. It needs Redis 6.2 or newer.
package redisstream
