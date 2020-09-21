package com.grakra.util

import java.sql.Connection
import java.util.concurrent.ArrayBlockingQueue
import com.grakra.util.Result
import java.util.concurrent.atomic.AtomicInteger

class ConnectionPool<T : Connection>(
    initCb: () -> Unit,
    private val newCxnCb: () -> T,
    private val capacity: Int) {

  init {
    initCb()
  }

  var total = AtomicInteger(0)
  val queue = ArrayBlockingQueue<T>(
      if (capacity <= 0) 1 else capacity
  )

  val ok = AtomicInteger(0)
  val err = AtomicInteger(0)

  private fun getCxn():T {
    var cxn = queue.poll()
    if (cxn == null || capacity <= 0) {
      return newCxnCb()
    }
    return cxn
  }

  private fun returnCxn(cxn:T) {
    if (capacity <= 0 || !queue.offer(cxn)) {
      Result.b.wrap {
        cxn.close()
      }
    }
  }

  fun <R> exec(cb: (T) -> R): Result<R> {
    val cxn = getCxn()
    return Result.b wrap {
      cb(cxn)
    } onErr {
      err.incrementAndGet()
    } onOk {
      ok.incrementAndGet()
      returnCxn(cxn)
    } onAny {
      total.incrementAndGet()
    }
  }
}