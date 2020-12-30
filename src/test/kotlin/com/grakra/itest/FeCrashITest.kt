package com.grakra.itest

import com.grakra.TestMethodCapture
import com.grakra.util.HouseKeeper
import com.grakra.util.Util
import io.netty.util.concurrent.Promise
import org.junit.Assert
import org.testng.annotations.Listeners
import org.testng.annotations.Test
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.LockSupport
import kotlin.concurrent.thread
import kotlin.math.abs

@Listeners(TestMethodCapture::class)
class FeCrashITest : KotlinITest() {
  @Test
  fun testNormal() {
    do_test { cluster ->
      cluster.must_start_doris_cluster()
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
            "db0", "table3", emptyList(), data)
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

      println("clean doris cluster")
      cluster.clean_doris_cluster()
      println("bootstrap doris cluster")
      cluster.bootstrap_doris_cluster()

      println("create table db0.table3")
      val table3 = this.javaClass.classLoader.getResourceAsStream("table3.sql").bufferedReader().readText()
      val masterFe = cluster.getMasterFe()!!
      val jdbc = cluster.jdbc(masterFe)
      jdbc.queryTimeout = 10
      cluster.runSql(jdbc) { jdbc ->
        jdbc.execute("create database if not exists db0")
        jdbc.execute("use db0")
        jdbc.execute("set property for 'root' 'max_user_connections'='1000'")
        jdbc.execute(table3)
      }

      Assert.assertTrue(cluster.getAliveBeSet().size >= 3)
      val houseKeeper = HouseKeeper()
      val data = (
          "2017-07-03|1|1|jim|2\n" +
              "2017-07-05|2|1|grace|2\n" +
              "2017-07-08|3|2|tom|2\n" +
              "2017-07-10|4|3|bush|3\n" +
              "2017-07-07|5|3|helen|3").toByteArray()

      val killThread = AtomicBoolean(false)
      val totalBatch = AtomicInteger(0)
      val failBatch = AtomicInteger(0)
      val sucessBatch = AtomicInteger(0)
      val thd = thread {

        val promises = mutableListOf<Promise<Boolean?>>()
        while (!killThread.acquire && totalBatch.getAcquire() < 10) {
          totalBatch.incrementAndGet()
          houseKeeper.async {
            cluster.streamLoad(
                "doris_be1", 8040, "root", "", "label_${abs(Random().nextLong() / 2)}", "|",
                "db0", "table3", emptyList(), data)
          }.addListener { future ->
            if (future != null && future.isSuccess) {
              sucessBatch.incrementAndGet()
            } else {
              failBatch.incrementAndGet()
              future?.cause()?.printStackTrace()
            }
          }.let {
            promises.add(it)
          }

          if (promises.size == 1) {
            println("streamLoad for the ${totalBatch.getAcquire()} time")
            promises.forEach {
              it.get()
            }
            promises.clear()
          }
          LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1))
        }
        println("streamLoad for the ${totalBatch.getAcquire()} time")
        if (promises.isNotEmpty()) {
          promises.forEach { it.get() }
        }
      }

      println("stop doris_be0")
      val be = "doris_be0"
      cluster.ensure_doris_be_started(be)
      cluster.stop_doris_be(be)
      LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(20))

      println("bootstrap doris_be3")
      cluster.bootstrap_doris_be("doris_be3")

      println("wait for bad tablet replicas to be repaired")
      val repairSuccess = Util.timed(1200, 5, TimeUnit.SECONDS) {
        val badReplicas = cluster.getBadTabletReplicas("db0", "table3")
        println("PhaseI: Repair tablet replicas of db0.table3: num=${badReplicas?.size ?: "UNKNOWN"}")
        badReplicas?.isEmpty() ?: false
      }

      Assert.assertTrue(repairSuccess)
      println("start $be")
      cluster.start_doris_be(be)
      val repairSuccess2 = Util.timed(120, 5, TimeUnit.SECONDS) {
        val badReplicas = cluster.getBadTabletReplicas("db0", "table3")
        println("PhaseII: Repair tablet replicas of db0.table3: num=${badReplicas?.size ?: "UNKNOWN"}")
        badReplicas?.isEmpty() ?: false
      }
      Assert.assertTrue(repairSuccess2)
      println("kill thread")
      killThread.setRelease(true)
      println("wait thread")
      thd.join()
      println("wait houseKeeper shutdown")
      houseKeeper.shutdown()
      println("totalBatch=${totalBatch.getAcquire()}, successBatch=${sucessBatch.getAcquire()}, failBatch=${totalBatch.getAcquire()}")
      Assert.assertEquals(totalBatch.getAcquire(), sucessBatch.getAcquire() + failBatch.getAcquire())
    }
  }

  @Test
  fun testGetMasterFe() {
    do_test { cluster ->
      println("masterFe=${cluster.getMasterFe()}")
    }
  }

  @Test
  fun testAddNewBeAfterNetworkPartition() {

  }

  @Test
  fun test2() {
    do_test { }
  }

  @Test
  fun testGetBadTabletReplicas() {
    do_test { cluster ->
      val thd = thread {
        Util.timed(1200, 2, TimeUnit.SECONDS) {
          val badReplicas = cluster.getBadTabletReplicas("db0", "table3")
          println("badReplicas num=${badReplicas?.size ?: "UNKNOWN"}")
          false
        }
      }
      LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(10))
      thd.join()
    }
  }

  @Test
  fun testSpecifiedShortKeyLength() {
    do_test { cluster ->
      //cluster.bootstrap_doris_cluster()
      cluster.runSql { jdbc ->
        jdbc.execute("create database if not exists db0")
        val table4CreateSql = this.javaClass.classLoader.getResourceAsStream("table4.sql").bufferedReader().readText()
        jdbc.execute(table4CreateSql)
      }
    }
  }

  @Test
  fun testShowTable() {
    do_test { cluster ->
      cluster.runSql("db0") { j ->
        j.queryForList("""
          show tables;
        """.trimIndent()
        ).forEach(::println)
      }
    }
  }

  @Test
  fun testBitmapCountDistinct() {
    do_test { cluster ->
      //cluster.bootstrap_doris_cluster()
      cluster.runSql("db0") { jdbc ->
        jdbc.execute("""
          DROP TABLE IF EXISTS `page_uv`;
        """.trimIndent())

        jdbc.execute("""
          CREATE TABLE `page_uv` (
            `page_id` INT NOT NULL COMMENT '页面id',
            `visit_date` datetime NOT NULL COMMENT '访问时间',
            `visit_users` BITMAP BITMAP_UNION NOT NULL COMMENT '访问用户id'
          ) ENGINE=OLAP
          AGGREGATE KEY(`page_id`, `visit_date`)
          DISTRIBUTED BY HASH(`page_id`) BUCKETS 1
          PROPERTIES (
            "replication_num" = "1",
            "storage_format" = "DEFAULT"
          ); 
        """.trimIndent())

        jdbc.execute("""
          insert into db0.page_uv values 
          (1, '2020-06-23 01:30:30', to_bitmap(13)),
          (1, '2020-06-23 01:30:30', to_bitmap(23)),
          (1, '2020-06-23 01:30:30', to_bitmap(33)),
          (1, '2020-06-23 02:30:30', to_bitmap(13)),
          (2, '2020-06-23 01:30:30', to_bitmap(23));
        """.trimIndent())

        jdbc.queryForList("""
          select page_id, count(distinct visit_users) from db0.page_uv group by page_id;
        """.trimIndent()
        ).forEach(::println)

        cluster.streamLoad(
            "doris_be0",
            8040,
            "root",
            "",
            "label_${System.currentTimeMillis()}",
            ",",
            "db0",
            "page_uv",
            listOf("columns" to "page_id,visit_date,visit_users, visit_users=to_bitmap(visit_users)"),
            """
              1,2020-06-23 01:30:30,130
              1,2020-06-23 01:30:30,230
              1,2020-06-23 01:30:30,120
              1,2020-06-23 02:30:30,133
              2,2020-06-23 01:30:30,234
            """.trimIndent().toByteArray()
        )

        jdbc.queryForList("""
          select page_id, count(distinct visit_users) from db0.page_uv group by page_id;
        """.trimIndent()
        ).forEach(::println)

        jdbc.queryForList("""
          explain select page_id, count(distinct visit_users) from db0.page_uv group by page_id;
        """.trimIndent()
        ).forEach(::println)
      }
    }
  }

  @Test
  fun testExplain() {
    do_test { c ->
      c.runSql("db0") { j ->
        j.queryForList("""
          explain select page_id, count(distinct visit_users) from db0.page_uv group by page_id;
        """.trimIndent()
        ).forEach(::println)
      }
    }
  }

  @Test
  fun testHLLUnion() {
    val db = "db0"
    do_test { c ->
      //c.bootstrap_doris_cluster()
      c.runSql() { j ->
        j.execute("create database if not exists $db")
      }
      c.runSql(db) { j ->

        j.execute("""
          DROP TABLE IF EXISTS TEST
        """.trimIndent())

        j.execute("""
          CREATE TABLE IF NOT EXISTS TEST(
                  dt DATE,
                  id INT,
                  uv HLL HLL_UNION
          ) 
          DISTRIBUTED BY HASH(ID) BUCKETS 32;
        """.trimIndent())
      }
      c.streamLoad(
          "doris_be0",
          8040,
          "root",
          "",
          "label_${System.currentTimeMillis()}",
          ",",
          db,
          "TEST",
          listOf("columns" to "dt,id,user_id, uv=hll_hash(user_id)"),
          """
             2020-06-23 01:30:30,1,1
             2020-06-23 01:30:30,1,2
             2020-06-23 01:30:30,2,2
             2020-06-23 02:30:30,2,2
             2020-06-23 01:30:30,3,2
          """.trimIndent().toByteArray()
      )
      c.runSql(db) { j ->
        j.queryForList("SELECT COUNT(DISTINCT uv) FROM TEST GROUP BY ID")
      }.forEach(::println)
    }
  }

  @Test
  fun testHLLImplementedBySQL() {
    val randInt = { n: Int ->
      val r = Random();
      { abs(r.nextInt(n)) }
    }(10000)

    val data = (1..100000).joinToString("\n") { i -> "$i,${randInt()}" }.toByteArray()
    val db = "db0"
    do_test { c ->
      c.must_start_doris_cluster()
      c.recreate_db(db)
      c.runSql(db) { j ->
        j.execute(
            """
              CREATE TABLE IF NOT EXISTS table0(
                c1 int,
                c2 int
              )
              DISTRIBUTED BY HASH(c1) BUCKETS 32; 
            """.trimIndent()
        )
      }

      c.runSql(db) { j ->
        j.execute(
            """
            CREATE TABLE IF NOT EXISTS table1(
              c1 int,
              c2 HLL HLL_UNION,
              c3 BITMAP BITMAP_UNION
            )
            DISTRIBUTED BY HASH(c1) BUCKETS 32; 
          """.trimIndent()
        )
      }

      c.streamLoad(db, "table0", data)
      c.streamLoad(db, "table1", listOf("columns" to "c1,c2,c3=to_bitmap(c2),c2=hll_hash(c2)"), data)
      //val hash1 = c.murmur_hash3_32(db, "table0")
      //val hash2 = c.murmur_hash3_32(db, "table1")
      //Assert.assertEquals(hash1, hash2)
      c.runSql(db) { j ->
        val estimate = "estimate"
        val count0 = j.queryForList("SELECT COUNT(DISTINCT c2) AS $estimate FROM $db.table0").first()[estimate] as Long
        val count1 = j.queryForList("SELECT NDV(c2) AS $estimate FROM $db.table0").first()[estimate] as Long
        val count2 = j.queryForList("SELECT COUNT(DISTINCT c2) AS $estimate FROM $db.table1").first()[estimate] as Long
        val count3 = j.queryForList("SELECT HLL_UNION_AGG(c2) AS $estimate FROM $db.table1").first()[estimate] as Long
        //val count4 = j.queryForList("SELECT BITMAP_UNION_AGG(c3) AS $estimate FROM $db.table1").first()[estimate] as Long
        val count5 = j.queryForList("SELECT COUNT(DISTINCT c3) AS $estimate FROM $db.table1").first()[estimate] as Long
        val count6 = j.queryForList(
            """
              SELECT floor((0.721 * 1024 * 1024) / (sum(pow(2, m * -1)) + 1024 - count(*))) AS estimate
              FROM(select(murmur_hash3_32(c2) & 1023) AS bucket,
                   max((31 - CAST(log2(murmur_hash3_32(c2) & 2147483647) AS INT))) AS m
                   FROM db0.table0
                   GROUP BY bucket) bucket_values
            """.trimIndent()).first()[estimate] as Long
        println("Traditional COUNT DISTINCT: $count0")
        println("Impala NDV: $count1")
        println("COUNT DISTINCT(implicit HLL_UNION_AGG): $count2")
        println("explicit HLL_UNION_AGG): $count3")
        //println("COUNT DISTINCT(implicit BITMAP_UNION_AGG): $count4")
        println("explicit BITMAP_UNION_AGG): $count5")
        println("HLL implemented by SQL: $count6")
      }
    }
  }

  @Test
  fun testRandomLong() {
    val randi = Util.randInt(1000)
    (0..10).forEach { _ ->
      println("${randi()}")
    }
  }

  @Test
  fun testLoadDataToBaseTimeThatHasOutOfSyncMV() {
    val db = "db0"
    val table = "table0"
    val randi = Util.randInt(1000)

    val data = (1..10000).joinToString("\n") { i ->
      "$i,${(1..5).map { randi() }.joinToString(",")}"
    }.toByteArray()

    do_test { c ->
      c.must_start_doris_cluster()
      c.recreate_db(db)
      c.runSql(db) { j ->
        j.execute("""
          CREATE TABLE IF NOT EXISTS $table (
            c1 BIGINT,
            c2 BIGINT,
            c3 BIGINT,
            c4 BIGINT,
            c5 BIGINT,
            c6 BIGINT
          )
          DISTRIBUTED BY HASH(c1,c2,c3,c4,c5) BUCKETS 32; 
        """.trimIndent())
      }

      val sqls = listOf(
          "mv1" to "create materialized view mv1 as select c1, sum(c6) from table0 group by c1",
          "mv2" to "create materialized view mv2 as select c1, c2, sum(c6) from table0 group by c1,c2",
          "mv3" to "create materialized view mv3 as select c1, c2, c3, sum(c6) from table0 group by c1,c2,c3",
          "mv4" to "create materialized view mv4 as select c1, c2, c3, c4, sum(c6) from table0 group by c1,c2,c3,c4",
          "mv5" to "create materialized view mv5 as select c2, sum(c6) from table0 group by c2",
          "mv6" to "create materialized view mv6 as select c3, sum(c6) from table0 group by c3",
          "mv7" to "create materialized view mv7 as select c4, sum(c6) from table0 group by c4",
          "mv8" to "create materialized view mv8 as select c5, sum(c6) from table0 group by c5",
          "mv9" to "create materialized view mv9 as select c2,c3, sum(c6) from table0 group by c2,c3"
      )

      sqls.take(6).forEach { (mv, sql) ->
        println("execute: $sql")
        c.runSql(db) { j ->
          j.execute(sql)
        }
        c.ensure_mv_finished(db, table, mv)
      }

      val loadData = {
        c.streamLoadIntoTableConcurrently(
            c.getAliveBeSet().toList(),
            db,
            table,
            listOf(),
            5,
            listOf(data)
        )
      }
      val cancel0 = loadData()
      LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(10))
      cancel0()
      val cancel1 = loadData()
      LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(10))

      sqls.drop(6).forEach { (mv, sql) ->
        println("execute: $sql")
        c.runSql(db) { j ->
          j.execute(sql)
        }
        c.ensure_mv_finished(db, table, mv)
      }
      cancel1()
    }
  }

  @Test
  fun testDropMV() {
    do_test { c ->
      c.must_start_doris_cluster()
      c.runSql("db0") { j ->
        j.execute("drop materialized view if exists mv1 ON table0")
        j.execute("drop materialized view if exists mv1 ON table0")
        j.execute("drop materialized view if exists mv1 ON table0")
      }
    }
  }
}
