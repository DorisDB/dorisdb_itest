package com.grakra.itest

import com.grakra.TestMethodCapture
import com.grakra.dorisdb.DorisDBCluster
import com.grakra.util.HouseKeeper
import com.grakra.util.MySQLUtil
import com.grakra.util.Util
import com.mysql.jdbc.Driver
import org.junit.Assert
import org.springframework.jdbc.datasource.SimpleDriverDataSource
import org.testng.annotations.Listeners
import org.testng.annotations.Test
import java.util.*
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.LockSupport
import kotlin.concurrent.thread
import kotlin.math.abs
import kotlin.test.assertEquals

@Listeners(TestMethodCapture::class)
class FeCrashITest : KotlinITest() {
  @Test
  fun testNormal() {
    do_test { cluster ->
      cluster.clean_doris_cluster()
      cluster.bootstrap_doris_cluster()
      //cluster.stop_doris_fe("doris_fe_follower0")
      //cluster.start_doris_fe("doris_fe_follower1")
    }
  }

  @Test
  fun testShowProcFrontends() {
    do_test { cluster ->
      val masterFe = cluster.getMasterFe("doris_fe_follower0")
      println("masterFe=$masterFe")
    }
  }

  @Test
  fun testMasterFeFailOver() {
    do_test { cluster ->
      var masterFe = cluster.getMasterFe()
      Assert.assertNotNull(masterFe)
      val user = cluster.runSql { jdbc ->
        jdbc.execute("CREATE USER 'test20' IDENTIFIED BY 'test';")
        val users = jdbc.queryForList("SHOW GRANTS FOR 'test20'@'%';")
        Assert.assertTrue(users.size == 1)
        users.first()
      }
      cluster.failover()

      val user2 = cluster.runSql { jdbc ->
        val users = jdbc.queryForList("SHOW GRANTS FOR 'test20'@'%';")
        Assert.assertTrue(users.size == 1)
        users.first()
      }
      Assert.assertEquals(user, user2)
      masterFe = cluster.getMasterFe()
      Assert.assertNotNull(masterFe)
    }
  }

  @Test
  fun testCreateTable2() {
    do_test { cluster ->
      val table2 = this.javaClass.classLoader.getResourceAsStream("table2.sql").bufferedReader().readText()
      cluster.runSql { jdbc ->
        jdbc.execute("create database if not exists db0")
        jdbc.execute("use db0")
        jdbc.execute(table2)
      }
    }
  }

  @Test
  fun testCreateTable3() {
    do_test { cluster ->
      val table3 = this.javaClass.classLoader.getResourceAsStream("table3.sql").bufferedReader().readText()
      cluster.runSql { jdbc ->
        jdbc.execute("create database if not exists db0")
        jdbc.execute("use db0")
        jdbc.execute(table3)
      }
    }
  }

  @Test
  fun testEmptyTable() {
    do_test { cluster ->
      val rs = cluster.runSql { jdbc ->
        val sql = "select event_day, siteid, citycode, username, sum(pv) as pv_sum  from db0.table3 group by event_day, siteid, citycode, username;"
        jdbc.queryForList(sql)
      }
      Assert.assertTrue(rs.isEmpty())
    }
  }

  @Test
  fun testStreamLoad() {
    do_test { cluster ->
      val houseKeeper = HouseKeeper()
      val data = ("2017-07-03|1|1|jim|2\n" +
          "2017-07-05|2|1|grace|2\n" +
          "2017-07-08|3|2|tom|2\n" +
          "2017-07-10|4|3|bush|3\n" +
          "2017-07-07|5|3|helen|3").toByteArray()

      val promise = houseKeeper.async {
        cluster.streamLoad(
            "doris_be0", 8040, "root", "", "label_${abs(Random().nextLong() / 2)}", "|",
            "db0", "table3", data)
      }
      promise.get()
      val sql = "select event_day, sum(pv) as pv_sum from db0.table3 group by event_day order by event_day;"
      val rsBeforeFailOver = cluster.runSql { jdbc ->
        jdbc.queryForList(sql)
      }
      println("reBeforeFailOver=${rsBeforeFailOver}")
      cluster.failover()
      val rsAfter1FailOver = cluster.runSql { jdbc ->
        jdbc.queryForList(sql)
      }
      println("reAfter1FailOver=${rsAfter1FailOver}")
      cluster.failover()

      val rsAfter2FailOver = cluster.runSql { jdbc ->
        jdbc.queryForList(sql)
      }
      println("reAfter2FailOver=${rsAfter2FailOver}")
      Assert.assertEquals(rsAfter1FailOver, rsAfter2FailOver)
      Assert.assertEquals(rsBeforeFailOver, rsAfter2FailOver)
    }
  }

  @Test
  fun testNetworkPartition() {
    do_test { cluster ->
      val port = cluster.beConf.getProperty("heartbeat_service_port", "9050").toInt()
      val masterFe = cluster.getMasterFe()
      val sql = "select event_day, sum(pv) as pv_sum from db0.table3 group by event_day order by event_day;"
      Assert.assertNotNull(masterFe)

      val restores = Array<() -> Unit>(2) { i ->
        cluster.drop_inbound_packet(masterFe!!, "doris_be$i", port)
      }

      val rs1 = cluster.runSql { jdbc ->
        jdbc.queryForList(sql)
      }
      LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(20))
      val rs2 = cluster.runSql { jdbc ->
        jdbc.queryForList(sql)
      }

      restores.forEach { it() }

      LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(20))
      val rs3 = cluster.runSql { jdbc ->
        jdbc.queryForList(sql)
      }
      Assert.assertEquals(rs1, rs2)
      Assert.assertEquals(rs1, rs3)
    }
  }

  @Test
  fun testStreamLoadWhenMinorityOfBECrashes() {
    do_test { cluster ->

      Assert.assertTrue(cluster.getAliveBeSet().size >= 3)

      val houseKeeper = HouseKeeper()
      val data = ("2017-07-03|1|1|jim|2\n" +
          "2017-07-05|2|1|grace|2\n" +
          "2017-07-08|3|2|tom|2\n" +
          "2017-07-10|4|3|bush|3\n" +
          "2017-07-07|5|3|helen|3").toByteArray()

      val killThread = AtomicBoolean(false)
      val totalBatch = AtomicInteger(0)
      val failBatch = AtomicInteger(0)
      val sucessBatch = AtomicInteger(0)
      val thd = thread {
        while (killThread.acquire) {
          totalBatch.incrementAndGet()
          houseKeeper.async {
            cluster.streamLoad(
                "doris_be0", 8040, "root", "", "label_${abs(Random().nextLong() / 2)}", "|",
                "db0", "table3", data)
          }.addListener { future ->
            if (future != null && future.isSuccess) {
              sucessBatch.incrementAndGet()
            } else {
              failBatch.incrementAndGet()
              future?.cause()?.printStackTrace()
            }
          }
        }
      }
      val be = "doris_be0"
      cluster.ensure_doris_be_started(be)
      cluster.stop_doris_be(be)
      LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1200))

      killThread.setRelease(true)
      thd.join()
      houseKeeper.shutdown()
    }
  }

  @Test
  fun testAddNewBeAfterNetworkPartition() {

  }

  @Test
  fun test2() {
    do_test { }
  }
}