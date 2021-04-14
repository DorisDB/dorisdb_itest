package com.grakra.itest;

import com.grakra.TestMethodCapture;
import org.testng.Assert
import org.testng.annotations.Listeners;
import org.testng.annotations.Test
import java.io.File

@Listeners(TestMethodCapture::class)
class SystemCaseFailTest : DorisDBRemoteITest() {

    @Test
    fun test_access_array_element(){
        val db = "test_array_1613733367366"
        run_mysql {c->
            c.q{sql->
                sql.e("""
                    drop database if exists $db;
                    create database $db;
                """.trimIndent())
            }
            c.q(db){sql->
                sql.e("""
                    set vectorized_engine_enable=true;
                    set vectorized_insert_enable=true;
                """.trimIndent())
                sql.q("""
                    select [1, 2.0, '3', null]
                """.trimIndent())!!.forEach { rows ->
                    rows.entries.joinToString { (k, v) -> "$k=$v" }.let { println(it) }
                }
            }
        }
    }
    @Test
    fun TestAlterTableAddDecimalToPrefixIndex(){
        val db = "alter_table_test_db_1613639289"
        val data = File("./basic_types_data").inputStream().readBytes()
        run_mysql {c->
            c.q{sql->
                sql.e("""
                    drop database if exists $db;
                    create database $db
                """.trimIndent())
            }
            c.q(db){sql ->
                sql.e("""
                     CREATE TABLE `duplicate_table_with_null` ( `k1`  date, `k2`  datetime, `k3`  char(20), `k4`  varchar(20), `k5`  boolean, `k6`  tinyint, `k7`  smallint, `k8`  int, `k9`  bigint, `k10` largeint, `k11` float, `k12` double, `k13` decimal(27,9) ) ENGINE=OLAP DUPLICATE KEY(`k1`, `k2`, `k3`, `k4`, `k5`) COMMENT "OLAP" DISTRIBUTED BY HASH(`k1`, `k2`, `k3`) BUCKETS 3 PROPERTIES ( "replication_num" = "1", "storage_format" = "v2" );
                """.trimIndent())
            }

            val status = StreamLoad.streamLoad("39.103.134.93", 8333, db,  "duplicate_table_with_null", "\t", data)
            Assert.assertTrue(status)

            c.q(db){sql->
                sql.q("""
                     select sum(cast(k6 as int)), sum(k7), sum(k8), max(k9), max(k10), min(k11), min(k12), sum(k13) from duplicate_table_with_null where k1 = '2020-06-23' and k2 <= '2020-06-23 18:11:00';
                """.trimIndent())!!.forEach { rows ->
                    rows.entries.joinToString { (k, v) -> "$k=$v" }.let { println(it) }
                }
                sql.e("""
                    ALTER TABLE duplicate_table_with_null ADD COLUMN add_key DECIMAL(27, 9) KEY FIRST
                """.trimIndent())
            }


            val status2 = StreamLoad.streamLoad("39.103.134.93", 8333, db,  "duplicate_table_with_null", "\t", data)
            Assert.assertTrue(status2)
            c.q(db) {sql->
                sql.q("""
                    SHOW COLUMNS FROM duplicate_table_with_null
                """.trimIndent())!!.forEach { rows ->
                    rows.entries.joinToString { (k, v) -> "$k=$v" }.let { println(it) }
                }
            }
            /*
            c.q{sql->
                sql.e("""
                    drop database $db
                """.trimIndent())
            }
             */
        }
    }
    fun test_common(query: String) {
        val db = "test_filter_1613649581356"
        run_mysql { c ->

            c.q { sql ->
                sql.e("drop database if exists $db")
                sql.e("create database $db")
            }
            c.q(db) { sql ->
                sql.e(
                        """
                             CREATE TABLE `t1` (
                              `t1a` varchar(20) NULL COMMENT "",
                              `t1b` smallint NULL COMMENT "",
                              `t1c` int null,
                              `t1d` bigint null,
                              `t1e` float null,
                              `t1f` double null,
                              `t1g` bigint null,
                              `t1h` datetime null,
                              `t1i` date
                            ) ENGINE=OLAP
                            DUPLICATE KEY(`t1a`)
                            DISTRIBUTED BY HASH(`t1a`) BUCKETS 3
                            PROPERTIES (
                            "replication_num" = "1",
                            "in_memory" = "false",
                            "storage_format" = "DEFAULT"
                            );
                        """.trimIndent()
                )

                sql.e("""
                    insert into t1 values
                      ("val1a", 6, 8, 10, 15.0, 20, 2000,  '2014-04-04 01:00:00', '2014-04-04'),
                      ("val1b", 8, 16, 19, 17.0, 25, 2600,  '2014-05-04 01:01:00', '2014-05-04'),
                      ("val1a", 16, 12, 21, 15.0, 20, 2000,  '2014-06-04 01:02:00', '2014-06-04'),
                      ("val1a", 16, 12, 10, 15.0,20, 2000,  '2014-07-04 01:01:00', '2014-07-04'),
                      ("val1c", 8, 16, 19, 17.0, 25, 2600,  '2014-05-04 01:02:00', '2014-05-05'),
                      ("val1d", null, 16, 22, 17.0, 25, 2600,  '2014-06-04 01:01:00', null),
                      ("val1d", null, 16, 19, 17.0, 25, 2600,  '2014-07-04 01:02:00', null),
                      ("val1e", 10, null, 25, 17.0, 25, 2600,  '2014-08-04 01:01:00', '2014-08-04'),
                      ("val1e", 10, null, 19, 17.0, 25, 2600,  '2014-09-04 01:02:00', '2014-09-04'),
                      ("val1d", 10, null, 12, 17.0, 25, 2600,  '2015-05-04 01:01:00', '2015-05-04'),
                      ("val1a", 6, 8, 10, 15.0, 20, 2000,  '2014-04-04 01:02:00,', '2014-04-04'),
                      ("val1e", 10, null, 19, 17.0, 25, 2600,  '2014-05-04 01:01:00', '2014-05-04');
                """.trimIndent())

                sql.e("""
                     CREATE TABLE `t2` (
                      `t2a` varchar(20) NULL COMMENT "",
                      `t2b` smallint NULL COMMENT "",
                      `t2c` int null,
                      `t2d` bigint null,
                      `t2e` float null,
                      `t2f` double null,
                      `t2g` varchar(20) null,
                      `t2h` datetime null,
                      `t2i` date
                    ) ENGINE=OLAP
                    DUPLICATE KEY(`t2a`)
                    DISTRIBUTED BY HASH(`t2a`) BUCKETS 3
                    PROPERTIES (
                    "replication_num" = "1",
                    "in_memory" = "false",
                    "storage_format" = "DEFAULT"
                    );
                """.trimIndent())
                sql.e("""
                    insert into t2 values
                      ("val2a", 6, 12, 14, 15, 20, 2000,  '2014-04-04 01:01:00', '2014-04-04'),
                      ("val1b", 10, 12, 19, 17, 25, 2600,  '2014-05-04 01:01:00', '2014-05-04'),
                      ("val1b", 8, 16, 119, 17, 25, 2600,  '2015-05-04 01:01:00', '2015-05-04'),
                      ("val1c", 12, 16, 219, 17, 25, 2600,  '2016-05-04 01:01:00', '2016-05-04'),
                      ("val1b", null, 16, 319, 17, 25, 2600,  '2017-05-04 01:01:00', null),
                      ("val2e", 8, null, 419, 17, 25, 2600,  '2014-06-04 01:01:00', '2014-06-04'),
                      ("val1f", 19, null, 519, 17, 25, 2600,  '2014-05-04 01:01:00', '2014-05-04'),
                      ("val1b", 10, 12, 19, 17, 25, 2600,  '2014-06-04 01:01:00', '2014-06-04'),
                      ("val1b", 8, 16, 19, 17, 25, 2600,  '2014-07-04 01:01:00', '2014-07-04'),
                      ("val1c", 12, 16, 19, 17, 25, 2600,  '2014-08-04 01:01:00', '2014-08-05'),
                      ("val1e", 8, null, 19, 17, 25, 2600,  '2014-09-04 01:01:00', '2014-09-04'),
                      ("val1f", 19, null, 19, 17, 25, 2600,  '2014-10-04 01:01:00', '2014-10-04'),
                      ("val1b", null, 16, 19, 17, 25, 2600,  '2014-05-04 01:01:00', null);
                """.trimIndent()
                )
                sql.e("""
                     CREATE TABLE `t3` (
                      `t3a` varchar(20) NULL COMMENT "",
                      `t3b` smallint NULL COMMENT "",
                      `t3c` int null,
                      `t3d` bigint null,
                      `t3e` float null,
                      `t3f` double null,
                      `t3g` varchar(20) null,
                      `t3h` datetime null,
                      `t3i` date
                    ) ENGINE=OLAP
                    DUPLICATE KEY(`t3a`)
                    DISTRIBUTED BY HASH(`t3a`) BUCKETS 3
                    PROPERTIES (
                    "replication_num" = "1",
                    "in_memory" = "false",
                    "storage_format" = "DEFAULT"
                    );
                """.trimIndent())
                sql.e("""
                    insert into t3 values
                      ("val3a", 6, 12, 110, 15, 20, 2000,  '2014-04-04 01:02:00', '2014-04-04'),
                      ("val3a", 6, 12, 10, 15, 20, 2000,  '2014-05-04 01:02:00','2014-05-04'),
                      ("val1b", 10, 12, 219, 17, 25, 2600,  '2014-05-04 01:02:00', '2014-05-04'),
                      ("val1b", 10, 12, 19, 17, 25, 2600,  '2014-05-04 01:02:00', '2014-05-04'),
                      ("val1b", 8, 16, 319, 17, 25, 2600,  '2014-06-04 01:02:00', '2014-06-04'),
                      ("val1b", 8, 16, 19, 17, 25, 2600,  '2014-07-04 01:02:00', '2014-07-04'),
                      ("val3c", 17, 16, 519, 17, 25, 2600,  '2014-08-04 01:02:00', '2014-08-04'),
                      ("val3c", 17, 16, 19, 17, 25, 2600,  '2014-09-04 01:02:00', '2014-09-05'),
                      ("val1b", null, 16, 419, 17, 25, 2600,  '2014-10-04 01:02:00', null),
                      ("val1b", null, 16, 19, 17, 25, 2600,  '2014-11-04 01:02:00', null),
                      ("val3b", 8, null, 719, 17, 25, 2600,  '2014-05-04 01:02:00', '2014-05-04'),
                      ("val3b", 8, null, 19, 17, 25, 2600,  '2015-05-04 01:02:00', '2015-05-04');
                """.trimIndent())
                sql.q(query)!!.forEach { rows ->
                    rows.entries.joinToString { (k, v) -> "$k=$v" }.let { println(it) }
                }
            }
            c.q { sql ->
                sql.e("""
                   drop database $db 
                """.trimIndent()
                )
            }
        }
    }

    @Test
    fun test_having() {
        val stmt = """
                SELECT t1a,
                       t1b,
                       t1h
                FROM   t1
                WHERE  t1b IN (SELECT t2b
                               FROM   t2
                               GROUP BY t2b
                               HAVING t2b < 10);
        """.trimIndent()
        test_common(stmt)
    }

    @Test
    fun test_in_group_by() {
        val stmt = """
                    SELECT t1a,
                           Avg(t1b)
                    FROM   t1
                    WHERE  t1a IN (SELECT t2a
                                   FROM   t2)
                    GROUP  BY t1a
                """.trimIndent()
        test_common(stmt)
    }

    @Test
    fun test_in_order_by() {
        val stmt = """
            SELECT t1a
            FROM   t1
            WHERE  t1b IN (SELECT t2b
                           FROM   t2
                           WHERE  t1a = t2a)
            ORDER  BY t1b DESC
        """.trimIndent()
        test_common(stmt)
    }

    @Test
    fun test_in_with_cte() {
        val stmt = """
            WITH cte1
                 AS (SELECT t1a,
                            t1b
                     FROM   t1
                     WHERE  t1a = "val1a")
            SELECT t1a,
                   t1b,
                   t1c,
                   t1d,
                   t1h
            FROM   t1
            WHERE  t1b IN (SELECT cte1.t1b
                           FROM   cte1
                           WHERE  cte1.t1b > 0)
        """.trimIndent()
        test_common(stmt)
    }

    @Test
    fun test_not_in_group_by() {
        val stmt = """
            SELECT t1a,
                   Sum(DISTINCT( t1b ))
            FROM   t1
            WHERE  t1d NOT IN (SELECT t2d
                               FROM   t2
                               WHERE  t1h < t2h)
            GROUP  BY t1a
        """.trimIndent()
        test_common(stmt)
    }

    @Test
    fun test_not_in_joins() {
        val stmt = """
            SELECT t1a,
                   t1b,
                   t1c,
                   t1d,
                   t1h
            FROM   t1
            WHERE  t1a NOT IN
                   (
                             SELECT    t2a
                             FROM      t2
                             LEFT JOIN t3 on t2b = t3b
                             WHERE t1d = t2d
                              )
            AND    t1d NOT IN
                   (
                          SELECT t2d
                          FROM   t2
                          RIGHT JOIN t1 on t2e = t1e
                          WHERE t1a = t2a)
        """.trimIndent()
        test_common(stmt)
    }

    @Test
    fun test_not_in_unit_tests_single_column() {
        val db = "test_filter_1613646918792"
        run_mysql { c ->
            c.q { sql ->
                sql.e("drop database if exists $db")
                sql.e("create database $db")
            }
            c.q(db) { sql ->
                sql.e("""
                    CREATE TABLE `m` (
                      `a` bigint NULL COMMENT "",
                      `b` double NULL COMMENT ""
                    ) ENGINE=OLAP
                    DUPLICATE KEY(`a`)
                    DISTRIBUTED BY HASH(`a`) BUCKETS 3
                    PROPERTIES (
                    "replication_num" = "1",
                    "in_memory" = "false",
                    "storage_format" = "DEFAULT"
                    )
                """.trimIndent())
                sql.e("""
                    insert into m VALUES
                      (null, 1.0),
                      (2, 3.0),
                      (4, 5.0)
                """.trimIndent())

                sql.e("""
                     CREATE TABLE `s` (
                      `c` bigint NULL COMMENT "",
                      `d` double NULL COMMENT ""
                    ) ENGINE=OLAP
                    DUPLICATE KEY(`c`)
                    DISTRIBUTED BY HASH(`c`) BUCKETS 3
                    PROPERTIES (
                    "replication_num" = "1",
                    "in_memory" = "false",
                    "storage_format" = "DEFAULT"
                    )
                """.trimIndent())

                sql.e("""
                    insert into s VALUES
                      (null, 1.0),
                      (2, 3.0),
                      (6, 7.0)
                """.trimIndent())
                sql.q("""
                      -- Uncorrelated NOT IN Subquery test cases
                      -- Case 1
                      -- (empty subquery -> all rows returned)
                    SELECT *
                    FROM   m
                    WHERE  a NOT IN (SELECT c
                                     FROM   s
                                     WHERE  d > 10.0) -- (empty subquer
                """.trimIndent())!!.forEach { rows ->
                    rows.entries.joinToString { (k, v) -> "$k=$v" }.let { println(it) }
                }
            }
            c.q { sql ->
                sql.e("drop database $db")
            }
        }
    }

    fun test_common2(vararg stmts: String) {
        val db = "join_test_1613646853"
        run_mysql { c ->
            c.q { sql ->
                sql.e("drop database if exists $db")
                sql.e("create database $db")
            }
            c.q(db) { sql ->
                sql.e("""
                    CREATE TABLE `t1` ( `t1_c1` int NOT NULL default "0", `t1_c2` int NOT NULL default "0", `t1_c3` int NOT NULL default "0", `t1_c4` varchar(10) NOT NULL default "", `t1_c5` varchar(10) NOT NULL default "", `t1_c6` varchar(10) NOT NULL default "" ) ENGINE=OLAP DUPLICATE KEY(`t1_c1`) COMMENT "OLAP" DISTRIBUTED BY HASH(`t1_c1`) PROPERTIES ( "replication_num" = "1", "storage_format" = "v2" );
                """.trimIndent())
                sql.e("""
                    CREATE TABLE `t2` ( `t2_c1` int NOT NULL default "0", `t2_c2` int NOT NULL default "0", `t2_c3` int NOT NULL default "0", `t2_c4` varchar(10) NOT NULL default "", `t2_c5` varchar(10) NOT NULL default "", `t2_c6` varchar(10) NOT NULL default "" ) ENGINE=OLAP DUPLICATE KEY(`t2_c1`) COMMENT "OLAP" DISTRIBUTED BY HASH(`t2_c1`) PROPERTIES ( "replication_num" = "1", "storage_format" = "v2" );
                """.trimIndent())
                sql.e("""
                    insert into join_test_1613646853.t1 (t1_c1, t1_c2)values (1, 111), (2, 222), (2, 22), (3, 33);
                    insert into join_test_1613646853.t2 (t2_c1, t2_c2)values (1, 113), (1, 112), (1, 22), (1, 11), (4, 44); 
                """.trimIndent())
                stmts.forEach { stmt ->
                    sql.q(stmt)!!.forEach { rows ->
                        rows.entries.joinToString { (k, v) -> "$k=$v" }.let { println(it) }
                    }
                }
            }
            c.q { sql ->
                sql.e("drop database if exists $db")
            }
        }
    }

    @Test
    fun test_simple_in() {
        test_common("""
            SELECT *
            FROM   t1
            WHERE  t1b IN (SELECT t2b
                           FROM   t2
                           WHERE  t1a = t2a)
        """.trimIndent())
    }

    @Test
    fun test_join_normal_join() {
        val stmts = arrayOf(
                "select * from (select t1_c1, t1_c2, t2_c1, t2_c2 from t1 inner join t2 on t1.t1_c1 = t2.t2_c1) as t3 order by t1_c1, t1_c2, t2_c1, t2_c2;",
                "select * from (select t1_c1, t1_c2, t2_c1, t2_c2 from t1 inner join t2 on t1.t1_c1 = t2.t2_c1 and t1.t1_c2 > t2.t2_c2) as t3 order by t1_c1, t1_c2, t2_c1, t2_c2;",
                "select * from (select t1_c1, t1_c2, t2_c1, t2_c2 from t1 left outer join t2 on t1.t1_c1 = t2.t2_c1) as t3 order by t1_c1, t1_c2, t2_c1, t2_c2;",
                "select * from (select t1_c1, t1_c2, t2_c1, t2_c2 from t1 left outer join t2 on t1.t1_c1 = t2.t2_c1 and t1_c2 > t2_c2) as t3 order by t1_c1, t1_c2, t2_c1, t2_c2;",
                "select t1_c1, t1_c2 from t1 left semi join t2 on t1.t1_c1=t2.t2_c1;",
                "select t1_c1, t1_c2 from t1 left semi join t2 on t1.t1_c1=t2.t2_c1 and t1.t1_c2>t2.t2_c2;",
                "select * from (select t1_c1, t1_c2 from t1 where t1_c1 not in (select t2_c1 from t2)) as t3 order by t1_c1, t1_c2;"
        )
        test_common2(*stmts);
    }

    @Test
    fun test_join_normal_join_nullable() {
        val db = "join_test_1613646855"
        run_mysql { c ->
            c.q { sql ->
                sql.e("drop database if exists $db")
                sql.e("create database $db")
            }
            c.q(db) { sql ->
                sql.e(
                        """
                            CREATE TABLE `nullable_t1` ( `t1_c1` int, `t1_c2` int, `t1_c3` int, `t1_c4` varchar(10), `t1_c5` varchar(10), `t1_c6` varchar(10) ) ENGINE=OLAP DUPLICATE KEY(`t1_c1`) COMMENT "OLAP" DISTRIBUTED BY HASH(`t1_c1`) PROPERTIES ( "replication_num" = "1", "storage_format" = "v2" );
                            CREATE TABLE `nullable_t2` ( `t2_c1` int, `t2_c2` int, `t2_c3` int, `t2_c4` varchar(10), `t2_c5` varchar(10), `t2_c6` varchar(10) ) ENGINE=OLAP DUPLICATE KEY(`t2_c1`) COMMENT "OLAP" DISTRIBUTED BY HASH(`t2_c1`) PROPERTIES ( "replication_num" = "1", "storage_format" = "v2" );
                            insert into join_test_1613646855.nullable_t1 (t1_c1, t1_c2)values (1, 111), (null, 7), (2, 222), (2, 22), (3, 33), (3, null);
                            insert into join_test_1613646855.nullable_t2 (t2_c1, t2_c2)values (1, 113), (null, 9), (1, 112), (1, 22), (1, 44), (4, 44), (5, null); 
                        """.trimIndent()
                )
                sql.q(
                        """
                            select t1_c1, t1_c2 from nullable_t1 as t1 where t1_c1 not in (select t2_c1 from nullable_t2 as t2) 
                        """.trimIndent())!!.forEach { rows ->
                    rows.entries.joinToString { (k, v) -> "$k=$v" }.let { println(it) }
                }
            }
            c.q {sql->
                sql.e("drop database if exists $db")
            }
        }
    }
}
