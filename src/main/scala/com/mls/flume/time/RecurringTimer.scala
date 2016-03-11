package com.mls.flume.time

import org.slf4j.LoggerFactory

/**
  * spark源码,实现定时操作的逻辑
  */
class RecurringTimer(clock: Clock, period: Long, callback: (Long) => Unit, name: String) {

  val logger = LoggerFactory.getLogger(classOf[RecurringTimer]);

  private val thread = new Thread("RecurringTimer - " + name) {
    setDaemon(true)

    override def run() {
      loop
    }
  }

  @volatile private var prevTime = -1L
  @volatile private var nextTime = -1L
  @volatile private var stopped = false

  /**
    * Get the time when this timer will fire if it is started right now.
    * The time will be a multiple of this timer's period and more than
    * current system time.
    */
  def getStartTime(): Long = {
    (math.floor(clock.getTimeMillis().toDouble / period) + 1).toLong * period
  }

  /**
    * Get the time when the timer will fire if it is restarted right now.
    * This time depends on when the timer was started the first time, and was stopped
    * for whatever reason. The time must be a multiple of this timer's period and
    * more than current time.
    */
  def getRestartTime(originalStartTime: Long): Long = {
    val gap = clock.getTimeMillis() - originalStartTime
    (math.floor(gap.toDouble / period).toLong + 1) * period + originalStartTime
  }

  /**
    * Start at the given start time.
    */
  def start(startTime: Long): Long = synchronized {
    nextTime = startTime
    thread.start()
    logger.debug("Started timer for " + name + " at time " + nextTime)
    nextTime
  }

  /**
    * Start at the earliest time it can start based on the period.
    */
  def start(): Long = {
    start(getStartTime())
  }

  /**
    * Stop the timer, and return the last time the callback was made.
    *
    * @param interruptTimer True will interrupt the callback if it is in progress (not guaranteed to
    *                       give correct time in this case). False guarantees that there will be at
    *                       least one callback after `stop` has been called.
    */
  def stop(interruptTimer: Boolean): Long = synchronized {
    if (!stopped) {
      stopped = true
      if (interruptTimer) {
        thread.interrupt()
      }
      thread.join()
      logger.debug("Stopped timer for " + name + " after time " + prevTime)
    }
    prevTime
  }

  private def triggerActionForNextInterval(): Unit = {
    clock.waitTillTime(nextTime)
    callback(nextTime)
    prevTime = nextTime
    nextTime += period
    logger.debug("Callback for " + name + " called at time " + prevTime)
  }

  /**
    * Repeatedly call the callback every interval.
    */
  private def loop() {
    try {
      while (!stopped) {
        triggerActionForNextInterval()
      }
      triggerActionForNextInterval()
    } catch {
      case e: InterruptedException =>
    }
  }
}

object RecurringTimer {
  val logger = LoggerFactory.getLogger(classOf[RecurringTimer]);

  def main(args: Array[String]) {
    var lastRecurTime = 0L
    val period = 1000

    def onRecur(time: Long) {
      val currentTime = System.currentTimeMillis()
      logger.debug("" + currentTime + ": " + (currentTime - lastRecurTime))
      lastRecurTime = currentTime
    }
    val timer = new RecurringTimer(new SystemClock(), period, onRecur, "Test")
    timer.start()
    Thread.sleep(30 * 1000)
    timer.stop(true)
  }
}
