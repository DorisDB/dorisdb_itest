package com.grakra.util

import org.testng.annotations.Test
import java.nio.charset.Charset

class TestRandUtil {
    @Test
    fun test0() {
        val tsGen = RandUtil.generateRandomTimestamp("2001-12-20 00:00:00", "2021-12-20 00:00:00")
        val dateGen = RandUtil.generateRandomDate("1990-01-01", "2021-12-31")
        val lowers = (0..25).map { ('a'.toInt() + it).toChar() }.toTypedArray()
        val uppers = (0..25).map { ('A'.toInt() + it).toChar() }.toTypedArray()
        val varcharGen = RandUtil.generateRandomVarChar(lowers + uppers, 5, 10)
        val tinyIntGen = RandUtil.generateRandomTinyInt(50)
        val smallIntGen = RandUtil.generateRandomSmallInt(50)
        val intGen = RandUtil.generateRandomInt(50)
        val bigIntGen = RandUtil.generateRandomBigInt(50)
        val booleanGen = RandUtil.generateRandomBoolean(50)
        (1..10).forEach { _ ->
            val ts = tsGen()
            val date = dateGen()
            val varchar = varcharGen().toString(Charsets.UTF_8)
            val tinyInt = tinyIntGen()
            val boolean = booleanGen()
            val smallInt = smallIntGen()
            val int = intGen()
            val bigInt = bigIntGen()
            println("ts=$ts, date=$date, varchar=$varchar, tinyInt=$tinyInt, boolean=$boolean, smallInt=$smallInt, int=$int, bigInt=$bigInt")
        }
    }

    @Test
    fun test(){
        val intGen = RandUtil.generateRandomInt(50)
        (0..100).forEach { _ -> println(intGen()) }
    }
}