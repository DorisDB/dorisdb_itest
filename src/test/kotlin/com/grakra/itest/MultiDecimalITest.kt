package com.grakra.itest

import com.grakra.TestMethodCapture
import com.grakra.util.Util
import org.testng.Assert
import org.testng.annotations.Listeners
import org.testng.annotations.Test

@Listeners(TestMethodCapture::class)
class MultiDecimalITest : DorisDBRemoteITest() {
    @Test
    fun testPing() {
        run_mysql { c ->
            val db = "decimal_test_db"
            val table = "decimal_table0"
            c.q { sql ->
                sql.e("create database if not exists $db")
                val result = sql.q("show databases")
                result!!.filter { row -> row.getValue("Database")!! == db }.count().let { n ->
                    Assert.assertTrue(n == 1)
                }
            }

            val tableSql = Util.getTableSql(table)
            c.q(db) { sql ->
                sql.e(tableSql)
                val result = sql.q("desc $table")
                println(result)
            }
        }
    }

    @Test
    fun testOrcVectorizedBrokerLoad() {
        val loadSql = """
            USE decimal_test_db;
            LOAD LABEL decimal_test_db.label_${System.currentTimeMillis()} (
                DATA INFILE("hdfs://172.26.92.141:9002/rpf/orc_files/foobar.orc")
                INTO TABLE `decimal_table0`
                FORMAT AS "orc"
                (id, decimal32, decimal64, decimal128, nullable_decimal32, nullable_decimal64, nullable_decimal128)
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
    fun testSelect() {
        val db = "decimal_test_db"
        val table = "decimal_table0"
        run_mysql{c->
            c.q(db){sql->
                sql.q("select * from $table limit 10")!!.forEach {rows->
                    rows.entries.joinToString { (k,v)->"$k=$v" }.let{println(it)}
                }
            }
        }
    }

    @Test
    fun testSelectAdd() {
        val db = "decimal_test_db"
        val table = "decimal_table0"
        run_mysql{c->
            c.q(db){sql->
                sql.q("select decimal32 as a , decimal32 as b, (decimal32 + decimal32) as result  from $table")!!.forEach {rows->
                    rows.entries.joinToString { (k,v)->"$k=$v" }.let{println(it)}
                }
            }
        }
    }
}
