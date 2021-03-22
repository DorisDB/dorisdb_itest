package com.grakra.itest

import com.google.common.base.Preconditions
import com.grakra.TestMethodCapture
import com.grakra.dorisdb.MySQLClient
import com.grakra.schema.ParquetUtil
import com.grakra.schema.SqlUtil
import com.grakra.schema.Table
import com.grakra.util.Util
import org.testng.Assert
import org.testng.annotations.Listeners


@Listeners(TestMethodCapture::class)
open class DorisDBRemoteITest : KotlinITest() {
    private val hostPort = System.getenv("DORISDB_FE_MASTER_HOSTPORT") ?: "39.103.134.93:8338"

    init {
        Preconditions.checkState(hostPort.matches(Regex("\\d+(\\.\\d+){3}:\\d+")))
    }

    private val mysql = MySQLClient(hostPort)
    fun <T> run_mysql(f: (MySQLClient) -> T): T {
        return f(mysql)
    }

    fun create_db(db: String) {
        run_mysql { c ->
            c.q { sql ->
                sql.e("drop database if exists $db")
                sql.e("create database if not exists $db")
                val result = sql.q("show databases")
                result!!.filter { row -> row.getValue("Database")!! == db }.count().let { n ->
                    Assert.assertTrue(n == 1)
                }
            }
        }
    }

    fun enable_decimal_v3(enable: Boolean) {
        val stmt = "set enable_decimal_v3 = $enable"
        run_mysql { c ->
            c.q { sql ->
                sql.e(stmt)
                val rs = sql.q("show variables")
                val r = rs!!.filter { row -> row.getValue("Variable_name")!! == "enable_decimal_v3" }.first()
                Assert.assertEquals(r.getValue("Value").toString(), enable.toString())
            }
        }
    }

    fun create_table(db: String, table: Table) {
        run_mysql { c ->
            val tableSql = table.sql()
            println("tableSql=$tableSql")
            c.q(db) { sql ->
                sql.e("set enable_decimal_v3 = true")
                sql.e(tableSql)
                val result = sql.q("desc ${table.tableName}")
                println(result)
            }
        }
    }

    fun admin_show_frontend_config(key: String): Map<String, Any> {
        var kvs: Map<String, Any>? = null
        run_mysql { c ->
            c.q { sql ->
                val rs = sql.q("admin show frontend config like '$key';")
                kvs = rs!!.map { r -> r["Key"] as String to r["Value"] as Any }.toMap()
            }
        }
        return kvs!!
    }

    fun admin_set_frontend_config(key: String, value: Any) {
        run_mysql { c ->
            c.q { sql ->
                sql.e("admin set frontend config (\"$key\" = \"$value\");")
            }
        }
    }

    fun admin_set_vectorized_load_enable(enable: Boolean) {
        val key = "vectorized_load_enable"
        admin_set_frontend_config(key, enable)
        val kvs = admin_show_frontend_config(key)
        Assert.assertTrue(kvs.containsKey(key))
        Assert.assertEquals(kvs.getValue(key), enable.toString())
    }

    fun query(db: String, stmt: String): List<Map<String, Any>> {
        println("query:\n$stmt")
        var rs: List<Map<String, Any>>? = null
        run_mysql { c ->
            c.q(db) { sql ->
                rs = sql.q(stmt)
            }
        }
        return rs!!
    }

    fun query_print(db: String, stmt: String) {
        println("query:\n$stmt")
        run_mysql { c ->
            c.q(db) { sql ->
                sql.qv(stmt)
            }
        }
    }

    fun execute(db: String, stmt: String) {
        run_mysql { c ->
            c.q(db) { sql ->
                sql.e(stmt)
            }
        }
    }

    fun compare_two_tables(db0: String, db1: String, table0: String, table1: String) {
        val fp0 = fingerprint_murmur_hash3_32(db0, "select * from $table0")
        val fp1 = fingerprint_murmur_hash3_32(db1, "select * from $table1")
        Assert.assertEquals(fp0, fp1)
    }

    fun fingerprint(db: String, hashFunc: String, sql: String): Long {
        val rs = query(db, SqlUtil.limit1(sql))
        Assert.assertTrue(rs.isNotEmpty())
        val colNames = rs.first().keys.toList()
        val fpRs = query(db, SqlUtil.fingerprint(hashFunc, colNames, sql))
        println(fpRs)
        Assert.assertTrue(fpRs.size == 1)
        return fpRs.first().getValue("fingerprint") as Long
    }

    fun fingerprint_murmur_hash3_32(db: String, sql: String) = fingerprint(db, "murmur_hash3_32", sql)

    fun compare_columns(db: String, table0: String, table1: String, vararg columnGroups: List<String>) {
        columnGroups.forEach { colGroup ->
            val colCvs = colGroup.joinToString(",")
            val fp0 = fingerprint_murmur_hash3_32(db, "select $colCvs from $table0")
            val fp1 = fingerprint_murmur_hash3_32(db, "select $colCvs from $table1")
            val result = if (fp0==fp1){"PASS"}else {"FAIL"}
            println("### [test column group($result)]:  ($colCvs): fp0=$fp0, fp1=$fp1")
            //Assert.assertEquals(fp0, fp1)
        }
    }

    fun compare_each_column(db:String, table0:String, table1:String, columns:List<String>){
        val columnGroups  = columns.map{listOf(it)}.toTypedArray()
        compare_columns(db, table0, table1, *columnGroups)
    }

    fun broker_load(db: String, table: Table, hdfsPath: String) {
        val loadSql = ParquetUtil.createParquetBrokerLoadSql(db, table, hdfsPath)
        broker_load(loadSql)
    }

    fun broker_load(loadSql: String) {
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