package com.grakra.itest

import com.grakra.TestMethodCapture
import org.testng.annotations.Listeners
import org.testng.annotations.Test

@Listeners(TestMethodCapture::class)
class ColocationJoinTest : KotlinITest() {

  @Test
  fun testCG() {
    val db = "db0"
    do_test { c ->
      c.must_start_doris_cluster()
      c.recreate_db(db)
      val sql0 = """
        CREATE TABLE table0 (
          k1 int,
          k2 char(20),
          k3 decimal(9,3),
          v1 int sum
        )
        DISTRIBUTED BY HASH(k1,k2,k3)
        BUCKETS 8
        PROPERTIES(
            "colocate_with" = "group0"
        );
      """.trimIndent()

      val sql1 = """
        CREATE TABLE table1 (
          k1 int,
          k2 char(20),
          k3 decimal(9,3),
          v1 int sum
        )
        DISTRIBUTED BY HASH(k2,k1,k3)
        BUCKETS 8
        PROPERTIES(
            "colocate_with" = "group0"
        );
      """.trimIndent()
      val sql2 = """
        CREATE TABLE table2 (
          ya_k1 int,
          ya_k2 char(20),
          ya_k3 decimal(9,3),
          v1 int sum
        )
        DISTRIBUTED BY HASH(ya_k1,ya_k2,ya_k3)
        BUCKETS 8
        PROPERTIES(
            "colocate_with" = "group0"
        );
      """.trimIndent()
      val sql3 = """
        CREATE TABLE table3 (
          k2 char(20),
          k1 int,
          k3 decimal(9,3),
          v1 int sum
        )
        DISTRIBUTED BY HASH(k2,k1,k3)
        BUCKETS 8
        PROPERTIES(
            "colocate_with" = "group0"
        );
      """.trimIndent()
      val sql4 = """
        CREATE TABLE table4 (
          ya_k2 char(20),
          ya_k1 int,
          ya_k3 decimal(9,3),
          v1 int sum
        )
        DISTRIBUTED BY HASH(ya_k2,ya_k1,ya_k3)
        BUCKETS 8
        PROPERTIES(
            "colocate_with" = "group0"
        );
      """.trimIndent()

      val sql5 = """
        CREATE TABLE table5 (
          ya_k2 char(20),
          ya_k1 int,
          ya_k3 decimal(9,3),
          v1 int sum
        )
        DISTRIBUTED BY HASH(ya_k2,ya_k1)
        BUCKETS 8
        PROPERTIES(
            "colocate_with" = "group0"
        );
      """.trimIndent()

      listOf(sql0, sql1, sql2, sql3, sql4).forEach { sql ->
        c.runSql(db) { j ->
          j.execute(sql)
        }
      }
      c.runSql(db) { j ->
        j.queryForList("show tables")
      }.forEach(::println)
    }
  }
}