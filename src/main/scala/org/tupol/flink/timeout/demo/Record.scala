package org.tupol.flink.timeout.demo

/**
 * Define a simple type for our records, having a string key and a random number, which we will use to
 * start a "timeConsumingOperation" and for all the demos in this package.
 * @param key some unique key
 * @param time how long should the job take
 * @param timestamp event time
 */
case class Record(key: String, time: Long, timestamp: Long = System.currentTimeMillis())

