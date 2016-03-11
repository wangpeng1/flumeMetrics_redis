package com.mls.flume.monitor

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

/**
  * Created by zhangzhikuan on 16/3/11.
  */
object Waiter {
  private val lock = new ReentrantLock()
  private val condition = lock.newCondition()
  // Guarded by "lock"
  private val stopped: Boolean = false

  /**
    * Return `true` if it's stopped; or throw the reported error if `notifyError` has been called; or
    * `false` if the waiting time detectably elapsed before return from the method.
    */
  def waitForStopOrError(timeout: Long = -1): Boolean = {
    lock.lock()
    try {
      if (timeout < 0) {
        while (!stopped) {
          condition.await()
        }
      } else {
        var nanos = TimeUnit.MILLISECONDS.toNanos(timeout)
        while (!stopped && nanos > 0) {
          nanos = condition.awaitNanos(nanos)
        }
      }
      // already stopped or timeout
      stopped
    } finally {
      lock.unlock()
    }
  }
}
