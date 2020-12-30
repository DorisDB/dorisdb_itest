package com.grakra.util;

import io.netty.channel.DefaultEventLoopGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

class HouseKeeper() {
  val group = DefaultEventLoopGroup()
  val choreScheduler = ScheduledThreadPoolExecutor(1);

  fun <V> chore(interval: Long, unit: TimeUnit, callable: () -> V) {
    choreScheduler.scheduleAtFixedRate(
        {
          async(callable)
        },
        interval, interval, unit)
  }

  fun <V> delayed(
      callable: () -> V, interval: Long, unit: TimeUnit,
      listener: GenericFutureListener<out Future<in V?>>) {
    choreScheduler.schedule({
      async(callable).addListener(listener)
    }, interval, unit)
  }

  fun <V> async(callable: () -> V): Promise<V?> {
    val exec = group.next()!!
    val promise = exec.newPromise<V>()!!
    exec.submit {
      Result.wrap {
        promise.setSuccess(callable());
      }.onErr {
        promise.setFailure(it)
      }
    }
    return promise
  }

  fun shutdown() {
    Result.wrap {
      choreScheduler.shutdown();
      choreScheduler.awaitTermination(0, TimeUnit.MILLISECONDS);
      group.shutdownGracefully();
      group.awaitTermination(0, TimeUnit.MILLISECONDS);
    }.onErr {
      it.printStackTrace();
    }
  }
}

