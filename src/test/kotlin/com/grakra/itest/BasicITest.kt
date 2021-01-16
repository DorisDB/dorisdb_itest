package com.grakra.itest

import com.grakra.TestMethodCapture
import com.grakra.dorisdb.DorisDBCluster
import org.testng.annotations.Listeners
import org.testng.annotations.Test

@Listeners(TestMethodCapture::class)
class BasicITest: KotlinITest() {
    @Test
    fun testPing(){
        do_test {c->
            val sql = c.getSqlClient("39.103.134.93:8338", null)
            sql.e("create database if not exists decimal_test")
            val result = sql.q("show databases")
            println(result)
        }
    }
}
