package com.grakra.itest

import com.grakra.TestMethodCapture
import com.grakra.util.Util
import org.junit.Assert
import org.testng.annotations.Listeners
import org.testng.annotations.Test
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.LockSupport

@Listeners(TestMethodCapture::class)
class MaterializedViewTest : KotlinITest() {
  fun prepareData() {
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
  fun testNormal() {
    prepareData()
  }

  @Test
  fun testDropMV() {
    prepareData()
    do_test { c ->
      c.dropMV("db0", "table0", "mv1")
    }
  }

  @Test
  fun testHllUnionMV() {
    val db = "db0"
    val table0 = "table0"
    val table1 = "table1"
    val randi = arrayOf(Util.randInt(4), Util.randInt(2), Util.randInt(Int.MAX_VALUE))
    val data = (0..10000).joinToString("\n") {
      (0..2).map { randi[it]() }.joinToString(",")
    }.toByteArray()

    do_test { c ->
      c.must_start_doris_cluster()
      c.recreate_db(db)
      c.dropTable(db, table0)
      c.dropTable(db, table1)
      val createTable0Sql = """
          CREATE TABLE IF NOT EXISTS $table0 (
            c1 BIGINT,
            c2 BIGINT,
            c3 BIGINT
          )
          DISTRIBUTED BY HASH(c1) BUCKETS 32;
      """.trimIndent()
      val createTable1Sql = """
          CREATE TABLE IF NOT EXISTS $table1 (
            c1 BIGINT,
            c2 BIGINT,
            c3 HLL HLL_UNION
          )
          AGGREGATE KEY(c1,c2)
          DISTRIBUTED BY HASH(c1) BUCKETS 32;
      """.trimIndent()

      val mv0 = "table0_mv0"
      val createMVSql = """
        CREATE MATERIALIZED VIEW $mv0 AS
        SELECT c1,c2,HLL_UNION(HLL_HASH(c3))
        FROM $table0
        GROUP BY c1,c2
      """.trimIndent()
      c.runSql(db) { j -> j.execute(createTable0Sql) }
      c.runSql(db) { j -> j.execute(createTable1Sql) }
      c.runSql(db) { j -> j.execute(createMVSql) }
      c.ensure_mv_finished(db, table0, mv0)
      c.streamLoad(db, table0, listOf(), data).let { ok -> Assert.assertTrue(ok) }
      c.streamLoad(db, table1, listOf("columns" to "c1,c2,c3, c3=HLL_HASH(c3)"), data).let { ok -> Assert.assertTrue(ok) }
      val rs0 = c.runSql(db) { j ->
        j.queryForList("select c1,c2, HLL_UNION_AGG(HLL_HASH(c3)) as uv from $table0 group by c1,c2")
      }
      val rs1 = c.runSql(db) { j ->
        j.queryForList("select c1,c2, count(distinct c3) as uv from $table1 group by c1,c2")
      }
      rs0.forEach(::println)
      rs1.forEach(::println)
      val rsMap0 = rs0.map { e -> "${e["c1"]}_${e["c2"]}" to e }.toMap()
      val rsMap1 = rs1.map { e -> "${e["c1"]}_${e["c2"]}" to e }.toMap()
      Assert.assertEquals(rsMap0, rsMap1)
    }
  }
}