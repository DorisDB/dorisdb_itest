package com.grakra.itest

import com.grakra.TestMethodCapture
import com.grakra.schema.FixedLengthType
import com.grakra.schema.SimpleField
import com.grakra.schema.Table
import com.grakra.util.Util
import org.testng.Assert
import org.testng.annotations.Listeners
import org.testng.annotations.Test
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.LockSupport

@Listeners(TestMethodCapture::class)
class DecimalV2ReserveTrailingZerosTest : DorisDBRemoteITest() {
    val db = "test_decimalv2_db"
    val tableName = "test_decimalv2_table"
    val table = Table(
            tableName,
            listOf(
                    SimpleField.fixedLength("id_int", FixedLengthType.TYPE_INT),
                    SimpleField.decimalv2("id_decimalv2", 27, 9)
            ),
            1)
    val columnNames = table.fields.map { it.name }

    @Test
    fun create_db_and_table() {
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
    fun test_reserve_trailing_zeros() {
        run_mysql { c ->
            c.q(db) { sql ->
                val insertStmt = "insert into $db.$tableName(${columnNames.joinToString(",")}) values (1, '12.00'), (2, '12.1'), (2, '12.001');"
                println(insertStmt)
                sql.e(insertStmt)
                sql.qv("select * from $tableName")
                sql.e("set enable_vectorized_engine=false")
                sql.qv("select * from $tableName")
            }
        }
    }
}