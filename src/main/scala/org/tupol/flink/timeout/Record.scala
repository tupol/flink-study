package org.tupol.flink.timeout


/**
 * Define a simple type for our records, having a string key and a random number, which we will use to
 * start a "timeConsumingOperation" and for all the demos in this package.
 * @param key
 * @param time
 */
case class Record(key: String, time: Long)
