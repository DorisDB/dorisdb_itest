package com.grakra.itest

import com.grakra.TestMethodCapture
import org.testng.Assert
import org.testng.annotations.Listeners
import org.testng.annotations.Test
import com.grakra.schema.FixedLengthType
import com.grakra.schema.OrcUtil
import com.grakra.schema.SimpleField
import com.grakra.schema.Table

@Listeners(TestMethodCapture::class)
class OrcLoadingColumnFromPathTest : DorisDBRemoteITest() {
    val db = "orc_db"
    val tableName = "orc_test_table0"
    val table = Table(tableName, listOf(
            SimpleField.fixedLength("col_int", FixedLengthType.TYPE_INT),
            SimpleField.varchar("col_varchar", 20),
            SimpleField.fixedLength("col_date", FixedLengthType.TYPE_DATE)),
            1)
    val columnFromPath = "col_date"

    @Test
    fun create_db_and_table(){
        run_mysql { c ->
            c.q { sql ->
                sql.e("drop database if exists $db")
                sql.e("create database if not exists $db")
                val result = sql.q("show databases")
                result!!.filter { row -> row.getValue("Database")!! == db }.count().let { n ->
                    Assert.assertTrue(n == 1)
                }
            }

            val tableSql = table.sql()
            println("tableSql=$tableSql")
            c.q(db) { sql ->
                sql.e(tableSql)
                val result = sql.q("desc $tableName")
                println(result)
            }
        }
    }

    @Test
    fun create_db_and_table_less_than(){
        run_mysql { c ->
            c.q { sql ->
                sql.e("drop database if exists $db")
                sql.e("create database if not exists $db")
                val result = sql.q("show databases")
                result!!.filter { row -> row.getValue("Database")!! == db }.count().let { n ->
                    Assert.assertTrue(n == 1)
                }
            }

            val tableSql = """
                DROP TABLE if exists orc_test_table0;
                CREATE TABLE if not exists orc_test_table0
                (
                col_int INT NOT NULL,
                col_varchar VARCHAR(20) NOT NULL,
                col_date INT NOT NULL
                ) 
                ENGINE=OLAP
                DUPLICATE KEY(`col_int`)
                PARTITION BY RANGE(col_date) (
                    PARTITION p20210307 VALUES less than('20210307'),
                    PARTITION p20210309 VALUES less than('20210309'),
                    PARTITION p20210308 VALUES less than('20210308'),
                    PARTITION p20210306 VALUES less than('20210306'),
                    PARTITION p20210310 VALUES less than('20210310')
                )
                DISTRIBUTED BY HASH(`col_int`) BUCKETS 1
                PROPERTIES(
                "replication_num" = "1",
                "in_memory" = "false",
                "storage_format" = "DEFAULT"
                );
            """.trimIndent()
            println("tableSql=$tableSql")
            c.q(db) { sql ->
                sql.e(tableSql)
                val result = sql.q("desc $tableName")
                println(result)
            }
        }
    }


    @Test
    fun create_db_and_table_dynamic_partition_by_int_key(){
        run_mysql { c ->
            c.q { sql ->
                sql.e("drop database if exists $db")
                sql.e("create database if not exists $db")
                val result = sql.q("show databases")
                result!!.filter { row -> row.getValue("Database")!! == db }.count().let { n ->
                    Assert.assertTrue(n == 1)
                }
            }

            val tableSql = """
                DROP TABLE if exists orc_test_table0;
                CREATE TABLE if not exists orc_test_table0
                (
                col_int INT NOT NULL,
                col_varchar VARCHAR(20) NOT NULL,
                col_date INT NOT NULL
                ) 
                ENGINE=OLAP
                DUPLICATE KEY(`col_int`)
                PARTITION BY RANGE(col_date) (
                    PARTITION p20210306 VALUES [("20210306"), ("20210307")),
                    PARTITION p20210307 VALUES [("20210307"), ("20210308")),
                    PARTITION p20210308 VALUES [("20210308"), ("20210309"))
                )
                DISTRIBUTED BY HASH(`col_int`) BUCKETS 1
                PROPERTIES(
                "replication_num" = "1",
                "dynamic_partition.enable"="true",
                "dynamic_partition.time_unit" = "DAY",
                "dynamic_partition.time_zone" = "Asia/Shanghai",
                "dynamic_partition.start"= "-2147483648",
                "dynamic_partition.end"= "3",
                "dynamic_partition.prefix"= "p",
                "dynamic_partition.buckets" = "1",
                "in_memory" = "false",
                "storage_format" = "DEFAULT"
                );
            """.trimIndent()
            println("tableSql=$tableSql")
            c.q(db) { sql ->
                sql.e(tableSql)
                val result = sql.q("desc $tableName")
                println(result)
            }
        }
    }

    @Test
    fun test_orc_vectorized_broker_load_dynamic_partition_by_int_key() {
        val loadSql = """
            USE orc_db;
            LOAD LABEL orc_db.label_${System.currentTimeMillis()} (
            DATA INFILE("hdfs://172.26.92.141:9002/rpf/orc_files/orc_test_table_dynamic_partition_by_int_key/col_date=*/*.orc")
            INTO TABLE `orc_test_table0`
            FORMAT AS "orc"
            (col_int, col_varchar)
            COLUMNS FROM PATH AS (col_date)
            SET (col_date=str_to_date(col_date, '%Y%m%d'))
            )
            WITH BROKER hdfs ("username"="root", "password"="");
        """.trimIndent()
        val checkLoadStateSql = "show load order by createtime desc limit 1"
        run_mysql { c ->
            c.q { sql ->
                sql.e(loadSql)
                loop@ while (true) {
                    val rs = sql.q(checkLoadStateSql)!!
                    println(rs.first())
                    val state = rs.first()["State"] as String
                    println("current state=$state")
                    when (state) {
                        "FINISHED" -> {
                            Assert.assertTrue(true, "Success load")
                            break@loop
                        }
                        "CANCELLED" -> {
                            Assert.fail("Failed load (cancelled)")
                        }
                    }
                    Thread.sleep(1000)
                }
            }
        }
    }

    @Test
    fun show_partition(){
        run_mysql {c->
            c.q(db){sql->
                sql.qv("show partitions from $tableName")
            }
        }
    }

    @Test
    fun create_orc_files(){
        val files = listOf("data01.orc", "data02.orc", "data03.orc")
        files.forEach { f ->
            OrcUtil.createOrcFile(f, table.keyFields(),table.valueFields(setOf(columnFromPath)), 409611, 4096)
        }
    }

    @Test
    fun testOrcVectorizedBrokerLoad() {
        //val loadSql = OrcUtil.createOrcBrokerLoadSql(db, table, "/rpf/orc_files/orc_test_table0/col_date=*/*.orc")
        //println(loadSql)
        val loadSql = """
            USE orc_db;
            LOAD LABEL orc_db.label_${System.currentTimeMillis()} (
            DATA INFILE("hdfs://172.26.92.141:9002/rpf/orc_files/orc_test_table0/col_date=*/*.orc")
            INTO TABLE `orc_test_table0`
            FORMAT AS "orc"
            (col_int, col_varchar)
            COLUMNS FROM PATH AS (col_date)
            SET (col_date=concat(col_date, '-01'))
            )
            WITH BROKER hdfs ("username"="root", "password"="");
        """.trimIndent()
        val checkLoadStateSql = "show load order by createtime desc limit 1"
        run_mysql { c ->
            c.q { sql ->
                sql.e(loadSql)
                loop@ while (true) {
                    val rs = sql.q(checkLoadStateSql)!!
                    println(rs.first())
                    val state = rs.first()["State"] as String
                    println("current state=$state")
                    when (state) {
                        "FINISHED" -> {
                            Assert.assertTrue(true, "Success load")
                            break@loop
                        }
                        "CANCELLED" -> {
                            Assert.fail("Failed load (cancelled)")
                        }
                    }
                    Thread.sleep(1000)
                }
            }
        }
    }

    @Test
    fun testOrcVectorizedBrokerLoad3() {
        //val loadSql = OrcUtil.createOrcBrokerLoadSql(db, table, "/rpf/orc_files/orc_test_table0/col_date=*/*.orc")
        //println(loadSql)
        val loadSql = """
            USE orc_db;
            LOAD LABEL orc_db.label_${System.currentTimeMillis()} (
            DATA INFILE("hdfs://172.26.92.141:9002/rpf/orc_files/orc_test_table2/col_date=*/*.orc")
            INTO TABLE `orc_test_table0`
            FORMAT AS "orc"
            (col_int, col_varchar)
            COLUMNS FROM PATH AS (col_date)
            SET (col_date=str_to_date(col_date, '%Y%m%d'))
            )
            WITH BROKER hdfs ("username"="root", "password"="");
        """.trimIndent()
        val checkLoadStateSql = "show load order by createtime desc limit 1"
        run_mysql { c ->
            c.q { sql ->
                sql.e(loadSql)
                loop@ while (true) {
                    val rs = sql.q(checkLoadStateSql)!!
                    println(rs.first())
                    val state = rs.first()["State"] as String
                    println("current state=$state")
                    when (state) {
                        "FINISHED" -> {
                            Assert.assertTrue(true, "Success load")
                            break@loop
                        }
                        "CANCELLED" -> {
                            Assert.fail("Failed load (cancelled)")
                        }
                    }
                    Thread.sleep(1000)
                }
            }
        }
    }

    @Test
    fun testOrcVectorizedBrokerLoad4() {
        //val loadSql = OrcUtil.createOrcBrokerLoadSql(db, table, "/rpf/orc_files/orc_test_table0/col_date=*/*.orc")
        //println(loadSql)
        val loadSql = """
            USE orc_db;
            LOAD LABEL orc_db.label_${System.currentTimeMillis()} (
            DATA INFILE("hdfs://172.26.92.141:9002/rpf/orc_files/orc_test_table2/col_date=*/*.orc")
            INTO TABLE `orc_test_table0`
            FORMAT AS "orc"
            (col_int, col_varchar)
            COLUMNS FROM PATH AS (col_date)
            SET (col_date=col_date)
            )
            WITH BROKER hdfs ("username"="root", "password"="");
        """.trimIndent()
        val checkLoadStateSql = "show load order by createtime desc limit 1"
        run_mysql { c ->
            c.q { sql ->
                sql.e(loadSql)
                loop@ while (true) {
                    val rs = sql.q(checkLoadStateSql)!!
                    println(rs.first())
                    val state = rs.first()["State"] as String
                    println("current state=$state")
                    when (state) {
                        "FINISHED" -> {
                            Assert.assertTrue(true, "Success load")
                            break@loop
                        }
                        "CANCELLED" -> {
                            Assert.fail("Failed load (cancelled)")
                        }
                    }
                    Thread.sleep(1000)
                }
            }
        }
    }

    @Test
    fun testOrcVectorizedBrokerLoad5() {
        //val loadSql = OrcUtil.createOrcBrokerLoadSql(db, table, "/rpf/orc_files/orc_test_table0/col_date=*/*.orc")
        //println(loadSql)
        val loadSql = """
            USE orc_db;
            LOAD LABEL orc_db.label_${System.currentTimeMillis()} (
            DATA INFILE("hdfs://172.26.92.141:9002/rpf/orc_files/orc_test_table3/col_date=*/*/*")
            INTO TABLE `orc_test_table0`
            FORMAT AS "orc"
            (col_int, col_varchar)
            COLUMNS FROM PATH AS (col_date)
            SET (col_date=col_date)
            )
            WITH BROKER hdfs ("username"="root", "password"="");
        """.trimIndent()
        val checkLoadStateSql = "show load order by createtime desc limit 1"
        run_mysql { c ->
            c.q { sql ->
                sql.e(loadSql)
                loop@ while (true) {
                    val rs = sql.q(checkLoadStateSql)!!
                    println(rs.first())
                    val state = rs.first()["State"] as String
                    println("current state=$state")
                    when (state) {
                        "FINISHED" -> {
                            Assert.assertTrue(true, "Success load")
                            break@loop
                        }
                        "CANCELLED" -> {
                            Assert.fail("Failed load (cancelled)")
                        }
                    }
                    Thread.sleep(1000)
                }
            }
        }
    }

    @Test
    fun testOrcVectorizedBrokerLoad2() {
        //val loadSql = OrcUtil.createOrcBrokerLoadSql(db, table, "/rpf/orc_files/orc_test_table0/col_date=*/*.orc")
        //println(loadSql)
        val loadSql = """
            USE orc_db;
            LOAD LABEL orc_db.label_1615219687600 (
            DATA INFILE("hdfs://172.26.92.141:9002/rpf/orc_files/orc_test_table1/col_date=*/*.orc")
            INTO TABLE `orc_test_table0`
            FORMAT AS "orc"
            (col_int, col_varchar)
            COLUMNS FROM PATH AS (col_date)
            )
            WITH BROKER hdfs ("username"="root", "password"="");
        """.trimIndent()
        val checkLoadStateSql = "show load order by createtime desc limit 1"
        run_mysql { c ->
            c.q { sql ->
                sql.e(loadSql)
                loop@ while (true) {
                    val rs = sql.q(checkLoadStateSql)!!
                    println(rs.first())
                    val state = rs.first()["State"] as String
                    println("current state=$state")
                    when (state) {
                        "FINISHED" -> {
                            Assert.assertTrue(true, "Success load")
                            break@loop
                        }
                        "CANCELLED" -> {
                            Assert.fail("Failed load (cancelled)")
                        }
                    }
                    Thread.sleep(1000)
                }
            }
        }
    }
}