package com.grakra.itest

import com.grakra.TestMethodCapture
import com.grakra.util.Util
import org.testng.annotations.Listeners
import org.testng.annotations.Test

@Listeners(TestMethodCapture::class)
class DorisTestFailTest : DorisDBRemoteITest() {
    @Test
    fun prepareData(){
        val createTableSqls = Util.listResource("doris_test/table", ".sql").map { it.readText()}
        val insertTableSqls = Util.listResource("doris_test/insert", ".sql").map {it.readText()}
        val db = "test"
        run_mysql {c->
            c.q{sql->
                sql.e("""
                    drop database if exists $db;
                    create database $db;
                """.trimIndent())
            }
            c.q(db){sql->
               createTableSqls.forEach {
                   println(it)
                   sql.e(it)
               }
               insertTableSqls.forEach {
                   println(it)
                   sql.e(it) }
            }
        }
    }
}
