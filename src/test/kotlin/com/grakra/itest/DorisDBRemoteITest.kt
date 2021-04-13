package com.grakra.itest

import com.google.common.base.Preconditions
import com.grakra.TestMethodCapture
import com.grakra.dorisdb.MySQLClient
import com.grakra.schema.OrcUtil
import com.grakra.schema.ParquetUtil
import com.grakra.schema.SqlUtil
import com.grakra.schema.Table
import com.grakra.util.Util
import org.testng.Assert
import org.testng.annotations.Listeners
import java.io.File
import java.util.concurrent.TimeUnit


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

    fun create_db_sql(db: String):String {
        return "drop database if exists $db;create database if not exists $db"
    }
    fun drop_db_sql(db:String):String {
        return "drop database if exists $db"
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
            c.q(db) { sql ->
                sql.e("set enable_decimal_v3 = true")
                sql.e(tableSql)
                sql.q("desc ${table.tableName}")
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

    fun check_alter_table_finished(db: String, table: Table) {
        val sql = table.showAlterTableColumnSql()
        Util.timed(60, 1, TimeUnit.SECONDS) {
            val rs = query(db, sql)
            println(rs)
            rs.size == 1 && rs.first().contains("State") && rs.first().getValue("State") == "FINISHED"
        }.let {
            Assert.assertTrue(it)
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
                sql.e("set enable_decimal_v3 = true")
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

    fun insert_values_sql(db: String, table: Table, rowsNum: Int, vararg customizedGenerators: Pair<String, () -> Any>): String {
        val orcPath = "$db.${table.tableName}.orc"
        OrcUtil.createOrcFile(
                orcPath,
                table.keyFields(),
                table.valueFields(setOf()),
                rowsNum,
                4096,
                *customizedGenerators)
        val fieldNames = table.fields.map { it.name }.toTypedArray()
        val tuples = OrcUtil.orcToList(orcPath, *fieldNames)
        // val csvTablesPath = "$csvTuplesPrefix/${table.tableName}.csv"
        // Util.createFile(File(csvTablesPath), tuples.joinToString("\n") { it.joinToString(", ") })
        return table.insertIntoValuesSql(tuples)
    }

    fun insert_values(db: String, table: Table, rowsNum: Int, vararg customizedGenerators: Pair<String, () -> Any>) {
        execute(db, insert_values_sql(db, table, rowsNum, *customizedGenerators))
    }

    fun insert_select(db: String, dstTable: Table, srcTable: Table) {
        val sql = dstTable.insertIntoSubQuerySql(srcTable.selectAll())
        execute(db, sql)
        val fp0 = fingerprint_murmur_hash3_32(db, dstTable.selectAll())
        val fp1 = fingerprint_murmur_hash3_32(db, srcTable.selectAll())
        Assert.assertEquals(fp0, fp1)
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

    fun fingerprint_sql(db: String, hashFunc: String, sql: String, vararg columns: String): String {
        val colNames = if (columns.isEmpty()) {
            val rs = query(db, SqlUtil.limit1(sql))
            Assert.assertTrue(rs.isNotEmpty())
            rs.first().keys.toList()
        } else {
            columns.toList()
        }
        return SqlUtil.fingerprint(hashFunc, colNames, sql)
    }


    fun fingerprint_murmur_hash3_32(db: String, sql: String) = fingerprint(db, "murmur_hash3_32", sql)
    fun fingerprint_murmur_hash3_32_sql(db: String, sql: String, vararg columns: String)
            = fingerprint_sql(db, "murmur_hash3_32", sql, *columns)

    fun compare_columns(db: String, table0: String, table1: String, vararg columnGroups: List<String>) {
        columnGroups.forEach { colGroup ->
            val colCvs = colGroup.joinToString(",")
            val fp0 = fingerprint_murmur_hash3_32(db, "select $colCvs from $table0")
            val fp1 = fingerprint_murmur_hash3_32(db, "select $colCvs from $table1")
            val result = if (fp0 == fp1) {
                "PASS"
            } else {
                "FAIL"
            }
            println("### [test column group($result)]:  ($colCvs): fp0=$fp0, fp1=$fp1")
            //Assert.assertEquals(fp0, fp1)
        }
    }

    fun compare_each_column(db: String, table0: String, table1: String, columns: List<String>) {
        val columnGroups = columns.map { listOf(it) }.toTypedArray()
        compare_columns(db, table0, table1, *columnGroups)
    }

    fun broker_load(db: String, table: Table, hdfsPath: String) {
        val loadSql = ParquetUtil.createParquetBrokerLoadSql(db, table, hdfsPath)
        broker_load(loadSql)
    }

    fun broker_load_and_compute_fingerprint(db: String, table: Table, format: String, hdfsPath: String) {
        val loadSql = table.brokerLoadSql(db, format, hdfsPath)
        create_table(db, table)
        broker_load(loadSql)
        println(fingerprint_murmur_hash3_32(db, "select * from ${table.tableName}"))
    }

    fun broker_load(loadSql: String) {
        val checkLoadStateSql = "show load order by createtime desc limit 1"
        run_mysql { c ->
            c.q { sql ->
                sql.e("set enable_decimal_v3 = true")
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