package com.grakra.itest

import com.grakra.TestMethodCapture
import com.grakra.schema.FunctionTestTable
import com.grakra.schema.OrcUtil
import com.grakra.util.Util
import org.testng.Assert
import org.testng.annotations.Listeners
import org.testng.annotations.Test

@Listeners(TestMethodCapture::class)
class FunctionTest : DorisDBRemoteITest() {

    val db = "decimal_test_db"
    val table = "decimal_all_types"

    @Test
    fun preapre_data() {
        FunctionTestTable.createOrcFile()
    }

    @Test
    fun test_create_table() {
        run_mysql { c ->
            c.q { sql ->
                sql.e("create database if not exists $db")
                val result = sql.q("show databases")
                result!!.filter { row -> row.getValue("Database")!! == db }.count().let { n ->
                    Assert.assertTrue(n == 1)
                }
            }

            val tableSql = FunctionTestTable.createTableSql(table)
            c.q(db) { sql ->
                sql.e(tableSql)
                val result = sql.q("desc $table")
                println(result)
            }
        }
    }

    @Test
    fun testOrcVectorizedBrokerLoad() {
        val loadSql = FunctionTestTable.brokerLoadSql(db, table)
        println(loadSql)
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

    fun query(stmt: String) {
        run_mysql { c ->
            c.q(db) { sql ->
                val resutl = sql.q(stmt)
                println("sql=${Util.squeezeWhiteSpaces(stmt)}")
                resutl!!.forEach { rows ->
                    rows.entries.joinToString { (k, v) -> "$k=$v" }.let { println(it) }
                }
            }
        }
    }

    @Test
    fun testDecimal32Add() {
        query("""
                    select 
                        col0_decimal_p9s2 as a, 
                        col0_decimal_p9s2 + col0_decimal_p9s2  as c
                    from $table 
                    limit 10;
                    """)
    }

    @Test
    fun testDecimal32AddConstInt() {
        query("""
                    select 
                        col0_decimal_p9s2 as a, 
                        col0_decimal_p9s2 + 10  as c
                    from $table 
                    limit 10;
                    """)
    }

    @Test
    fun testDecimal32AddConstFloat() {
        query("""
                    select 
                        col0_decimal_p9s2 as a
                        ,col0_decimal_p9s2 + cast(3.14 as decimal32(9,2)) as c0
                        ,col0_decimal_p9s2 + 3.14 as c1
                        ,3.14 + col0_decimal_p9s2 as c2
                        ,cast('3.14' as decimal32(9,2)) + col0_decimal_p9s2 as c3
                        ,cast('3.14' as float) + col0_decimal_p9s2 as c4
                        ,cast('3.14' as double) + col0_decimal_p9s2 as c5
                        ,cast(cast('3.14' as float) as decimal32(9,2)) + col0_decimal_p9s2 as c6
                        ,cast(cast('3.14' as double) as decimal32(9,2)) + col0_decimal_p9s2 as c7
                        ,cast(cast('3.14' as decimal) as decimal32(9,2)) + col0_decimal_p9s2 as c8
                        ,cast('3.14' as decimal64(18,2)) + col0_decimal_p9s2 as c9
                        ,cast('3.14' as decimal128(38,2)) + col0_decimal_p9s2 as c10
                        ,cast('3.14' as decimal128(38,29)) + col0_decimal_p9s2 as c11
                    from $table 
                    limit 10;
                    """)
    }

    @Test
    fun testDecimal32AddConstFloat2() {
        query("""
                    select 
                        col0_decimal_p9s2 as a
                        ,cast('3.14' as double) + col0_decimal_p9s2 as c1
                        ,cast('3.14' as float) + col0_decimal_p9s2 as c2
                        ,cast('3.14' as decimal32(9,2)) + col0_decimal_p9s2 as c3
                        ,3.14 + col0_decimal_p9s2 as c4
                    from $table 
                    limit 10;
                    """)
    }

    @Test
    fun testDecimal32AddConst3() {
        query("""
                    select 
                        col0_decimal_p9s2 as a
                        , (3.139  +  0.001 + col0_decimal_p9s2) as b
                    from $table 
                    limit 10;
                    """)
    }

    @Test
    fun testDecimalLiteralOperation() {
        query("""
                    select 
                          3.14 + 2.667 as add_result0
                        , cast(3.14 as decimal32(9,2)) + 2.667 as add_result1
                        , 3.14 + cast(2.667 as decimal32(9,2)) as add_result2
                        , cast('3.14' as decimal32(9,2)) + cast('2.667' as decimal32(9,3)) as add_result3
                        , cast('3.14' as decimal) + cast('2.667' as decimal) as add_result4
                        , cast(3.14 as decimal64(9,2)) + cast('2.667' as decimal64(9,3)) as add_result5
                        , cast(3.14 as decimal128(9,2)) + cast('2.667' as decimal128(9,3)) as add_result6
                        , cast(3.14 as decimal32(9,2)) + cast('2.667' as decimal64(18,3)) as add_result7
                        , cast(3.14 as decimal32(9,2)) + cast('2.667' as decimal128(18,3)) as add_result8
                        , cast(3.14 as decimal64(9,2)) + cast('2.667' as decimal128(18,3)) as add_result9
                        
                        , 3.14 - 2.667 as sub_result0
                        , cast(3.14 as decimal32(9,2)) - 2.667 as sub_result1
                        , 3.14 - cast(2.667 as decimal32(9,2)) as sub_result2
                        , cast('3.14' as decimal32(9,2)) - cast('2.667' as decimal32(9,3)) as sub_result3
                        , cast('3.14' as decimal) - cast('2.667' as decimal) as sub_result4
                        , cast(3.14 as decimal64(9,2)) - cast('2.667' as decimal64(9,3)) as sub_result5
                        , cast(3.14 as decimal128(9,2)) - cast('2.667' as decimal128(9,3)) as sub_result6
                        , cast(3.14 as decimal32(9,2)) - cast('2.667' as decimal64(18,3)) as sub_result7
                        , cast(3.14 as decimal32(9,2)) - cast('2.667' as decimal128(18,3)) as sub_result8
                        , cast(3.14 as decimal64(9,2)) - cast('2.667' as decimal128(18,3)) as sub_result9
                        
                        , 3.14 * 2.667 as mul_result0
                        , cast(3.14 as decimal32(9,2)) * 2.667 as mul_result1
                        , 3.14 * cast(2.667 as decimal32(9,2)) as mul_result2
                        , cast('3.14' as decimal32(9,2)) * cast('2.667' as decimal32(9,3)) as mul_result3
                        , cast('3.14' as decimal) * cast('2.667' as decimal) as sub_result4
                        , cast(3.14 as decimal64(9,2)) * cast('2.667' as decimal64(9,3)) as mul_result5
                        , cast(3.14 as decimal128(9,2)) * cast('2.667' as decimal128(9,3)) as mul_result6
                        , cast(3.14 as decimal32(9,2)) * cast('2.667' as decimal64(18,3)) as mul_result7
                        , cast(3.14 as decimal32(9,2)) * cast('2.667' as decimal128(18,3)) as mul_result8
                        , cast(3.14 as decimal64(9,2)) * cast('2.667' as decimal128(18,3)) as mul_result9
                        
                        , 3.14 / 2.667 as div_result0
                        , cast(3.14 as decimal32(9,2)) / 2.667 as div_result1
                        , 3.14 / cast(2.667 as decimal32(9,2)) as div_result2
                        , cast('3.14' as decimal32(9,2)) / cast('2.667' as decimal32(9,3)) as div_result3
                        , cast('3.14' as decimal) / cast('2.667' as decimal) as div_result4
                        , cast(3.14 as decimal64(9,2)) / cast('2.667' as decimal64(9,3)) as div_result5
                        , cast(3.14 as decimal128(9,2)) / cast('2.667' as decimal128(9,3)) as div_result6
                        , cast(3.14 as decimal32(9,2)) / cast('2.667' as decimal64(18,3)) as div_result7
                        , cast(3.14 as decimal32(9,2)) / cast('2.667' as decimal128(18,3)) as div_result8
                        , cast(3.14 as decimal64(9,2)) / cast('2.667' as decimal128(18,3)) as div_result9
                        
                        , 3.14 % 2.667 as mod_result0
                        , cast(3.14 as decimal32(9,2)) % 2.667 as mod_result1
                        , 3.14 % cast(2.667 as decimal32(9,2)) as mod_result2
                        , cast('3.14' as decimal32(9,2)) % cast('2.667' as decimal32(9,3)) as mod_result3
                        , cast('3.14' as decimal) % cast('2.667' as decimal) as mod_result4
                        , cast(3.14 as decimal64(9,2)) % cast('2.667' as decimal64(9,3)) as mod_result5
                        , cast(3.14 as decimal128(9,2)) % cast('2.667' as decimal128(9,3)) as mod_result6
                        , cast(3.14 as decimal32(9,2)) % cast('2.667' as decimal64(18,3)) as mod_result7
                        , cast(3.14 as decimal32(9,2)) % cast('2.667' as decimal128(18,3)) as mod_result8
                        , cast(3.14 as decimal64(9,2)) % cast('2.667' as decimal128(18,3)) as mod_result9
                        
                    """)
    }

    @Test
    fun testDecimalLiteralAddOtherLiteral() {
        query("""
                    select 
                      cast('12.3567' as decimal32(9,4)) + cast('200' as tinyint) as add_res0
                    , cast('12.3567' as decimal32(9,4)) + cast('200' as smallint) as add_res1
                    , cast('12.3567' as decimal32(9,4)) + cast('200' as int) as add_res2
                    , cast('12.3567' as decimal32(9,4)) + cast('200' as bigint) as add_res3
                    , cast('12.3567' as decimal32(9,4)) + cast('200.001' as float) as add_res4
                    , cast('12.3567' as decimal32(9,4)) + cast('200.001' as double) as add_res5
                    , cast('12.3567' as decimal32(9,4)) + cast('200.001' as decimal) as add_res6
                    , cast('200' as tinyint) + cast('12.3567' as decimal32(9,4)) as add_res7
                    , cast('200' as smallint) + cast('12.3567' as decimal32(9,4))  as add_res8
                    , cast('200' as int) + cast('12.3567' as decimal32(9,4)) as add_res9
                    , cast('200' as bigint) + cast('12.3567' as decimal32(9,4)) as add_res10
                    , cast('200.001' as float) + cast('12.3567' as decimal32(9,4)) as add_res11
                    , cast('200.001' as double) + cast('12.3567' as decimal32(9,4)) as add_res12
                    , cast('200.001' as decimal) + cast('12.3567' as decimal32(9,4)) as add_res13
                    """)
    }

    @Test
    fun testDecimalLiteralSubOtherLiteral() {
        query("""
                    select 
                      cast('12.3567' as decimal32(9,4)) - cast('200' as tinyint) as add_res0
                    , cast('12.3567' as decimal32(9,4)) - cast('200' as smallint) as add_res1
                    , cast('12.3567' as decimal32(9,4)) - cast('200' as int) as add_res2
                    , cast('12.3567' as decimal32(9,4)) - cast('200' as bigint) as add_res3
                    , cast('12.3567' as decimal32(9,4)) - cast('200.001' as float) as add_res4
                    , cast('12.3567' as decimal32(9,4)) - cast('200.001' as double) as add_res5
                    , cast('12.3567' as decimal32(9,4)) - cast('200.001' as decimal) as add_res6
                    , cast('200' as tinyint) - cast('12.3567' as decimal32(9,4)) as add_res7
                    , cast('200' as smallint) - cast('12.3567' as decimal32(9,4))  as add_res8
                    , cast('200' as int) - cast('12.3567' as decimal32(9,4)) as add_res9
                    , cast('200' as bigint) - cast('12.3567' as decimal32(9,4)) as add_res10
                    , cast('200.001' as float) - cast('12.3567' as decimal32(9,4)) as add_res11
                    , cast('200.001' as double) - cast('12.3567' as decimal32(9,4)) as add_res12
                    , cast('200.001' as decimal) - cast('12.3567' as decimal32(9,4)) as add_res13
                    """)
    }

    @Test
    fun testDecimalLiteralMulOtherLiteral() {
        query("""
                    select 
                      cast('12.3567' as decimal32(9,4)) * cast('200' as tinyint) as add_res0
                    , cast('12.3567' as decimal32(9,4)) * cast('200' as smallint) as add_res1
                    , cast('12.3567' as decimal32(9,4)) * cast('200' as int) as add_res2
                    , cast('12.3567' as decimal32(9,4)) * cast('200' as bigint) as add_res3
                    , cast('12.3567' as decimal32(9,4)) * cast('200.001' as float) as add_res4
                    , cast('12.3567' as decimal32(9,4)) * cast('200.001' as double) as add_res5
                    , cast('12.3567' as decimal32(9,4)) * cast('200.001' as decimal) as add_res6
                    , cast('200' as tinyint) * cast('12.3567' as decimal32(9,4)) as add_res7
                    , cast('200' as smallint) * cast('12.3567' as decimal32(9,4))  as add_res8
                    , cast('200' as int) * cast('12.3567' as decimal32(9,4)) as add_res9
                    , cast('200' as bigint) * cast('12.3567' as decimal32(9,4)) as add_res10
                    , cast('200.001' as float) * cast('12.3567' as decimal32(9,4)) as add_res11
                    , cast('200.001' as double) * cast('12.3567' as decimal32(9,4)) as add_res12
                    , cast('200.001' as decimal) * cast('12.3567' as decimal32(9,4)) as add_res13
                    """)
    }

    @Test
    fun testDecimalLiteralDivOtherLiteral() {
        query("""
                    select 
                      cast('12.3567' as decimal32(9,4)) / cast('200' as tinyint) as add_res0
                    , cast('12.3567' as decimal32(9,4)) / cast('200' as smallint) as add_res1
                    , cast('12.3567' as decimal32(9,4)) / cast('200' as int) as add_res2
                    , cast('12.3567' as decimal32(9,4)) / cast('200' as bigint) as add_res3
                    , cast('12.3567' as decimal32(9,4)) / cast('200.001' as float) as add_res4
                    , cast('12.3567' as decimal32(9,4)) / cast('200.001' as double) as add_res5
                    , cast('12.3567' as decimal32(9,4)) / cast('200.001' as decimal) as add_res6
                    , cast('200' as tinyint) / cast('12.3567' as decimal32(9,4)) as add_res7
                    , cast('200' as smallint) / cast('12.3567' as decimal32(9,4))  as add_res8
                    , cast('200' as int) / cast('12.3567' as decimal32(9,4)) as add_res9
                    , cast('200' as bigint) / cast('12.3567' as decimal32(9,4)) as add_res10
                    , cast('200.001' as float) / cast('12.3567' as decimal32(9,4)) as add_res11
                    , cast('200.001' as double) / cast('12.3567' as decimal32(9,4)) as add_res12
                    , cast('200.001' as decimal) / cast('12.3567' as decimal32(9,4)) as add_res13
                    """)
    }

    @Test
    fun testDecimalV2LiteralAddOtherLiteral() {
        query("""
                    select 
                      cast('12.3567' as decimal) + cast('200' as tinyint) as add_res0
                    , cast('12.3567' as decimal) + cast('200' as smallint) as add_res1
                    , cast('12.3567' as decimal) + cast('200' as int) as add_res2
                    , cast('12.3567' as decimal) + cast('200' as bigint) as add_res3
                    , cast('12.3567' as decimal) + cast('200.001' as float) as add_res4
                    , cast('12.3567' as decimal) + cast('200.001' as double) as add_res5
                    , cast('12.3567' as decimal) + cast('200.001' as decimal) as add_res6
                    , cast('200' as tinyint) + cast('12.3567' as decimal) as add_res7
                    , cast('200' as smallint) + cast('12.3567' as decimal)  as add_res8
                    , cast('200' as int) + cast('12.3567' as decimal) as add_res9
                    , cast('200' as bigint) + cast('12.3567' as decimal) as add_res10
                    , cast('200.001' as float) + cast('12.3567' as decimal) as add_res11
                    , cast('200.001' as double) + cast('12.3567' as decimal) as add_res12
                    , cast('200.001' as decimal) + cast('12.3567' as decimal) as add_res13
                    """)
    }

    @Test
    fun testDecimalV2LiteralSubOtherLiteral() {
        query("""
                    select 
                      cast('12.3567' as decimal) - cast('200' as tinyint) as add_res0
                    , cast('12.3567' as decimal) - cast('200' as smallint) as add_res1
                    , cast('12.3567' as decimal) - cast('200' as int) as add_res2
                    , cast('12.3567' as decimal) - cast('200' as bigint) as add_res3
                    , cast('12.3567' as decimal) - cast('200.001' as float) as add_res4
                    , cast('12.3567' as decimal) - cast('200.001' as double) as add_res5
                    , cast('12.3567' as decimal) - cast('200.001' as decimal) as add_res6
                    , cast('200' as tinyint) - cast('12.3567' as decimal) as add_res7
                    , cast('200' as smallint) - cast('12.3567' as decimal)  as add_res8
                    , cast('200' as int) - cast('12.3567' as decimal) as add_res9
                    , cast('200' as bigint) - cast('12.3567' as decimal) as add_res10
                    , cast('200.001' as float) - cast('12.3567' as decimal) as add_res11
                    , cast('200.001' as double) - cast('12.3567' as decimal) as add_res12
                    , cast('200.001' as decimal) - cast('12.3567' as decimal) as add_res13
                    """)
    }

    @Test
    fun testDecimalV2LiteralMulOtherLiteral() {
        query("""
                    select 
                      cast('12.3567' as decimal) * cast('200' as tinyint) as add_res0
                    , cast('12.3567' as decimal) * cast('200' as smallint) as add_res1
                    , cast('12.3567' as decimal) * cast('200' as int) as add_res2
                    , cast('12.3567' as decimal) * cast('200' as bigint) as add_res3
                    , cast('12.3567' as decimal) * cast('200.001' as float) as add_res4
                    , cast('12.3567' as decimal) * cast('200.001' as double) as add_res5
                    , cast('12.3567' as decimal) * cast('200.001' as decimal) as add_res6
                    , cast('200' as tinyint) * cast('12.3567' as decimal) as add_res7
                    , cast('200' as smallint) * cast('12.3567' as decimal)  as add_res8
                    , cast('200' as int) * cast('12.3567' as decimal) as add_res9
                    , cast('200' as bigint) * cast('12.3567' as decimal) as add_res10
                    , cast('200.001' as float) * cast('12.3567' as decimal) as add_res11
                    , cast('200.001' as double) * cast('12.3567' as decimal) as add_res12
                    , cast('200.001' as decimal) * cast('12.3567' as decimal) as add_res13
                    """)
    }

    @Test
    fun testDecimalV2LiteralDivOtherLiteral() {
        query("""
                    select 
                      cast('12.3567' as decimal) / cast('200' as tinyint) as add_res0
                    , cast('12.3567' as decimal) / cast('200' as smallint) as add_res1
                    , cast('12.3567' as decimal) / cast('200' as int) as add_res2
                    , cast('12.3567' as decimal) / cast('200' as bigint) as add_res3
                    , cast('12.3567' as decimal) / cast('200.001' as float) as add_res4
                    , cast('12.3567' as decimal) / cast('200.001' as double) as add_res5
                    , cast('12.3567' as decimal) / cast('200.001' as decimal) as add_res6
                    , cast('200' as tinyint) / cast('12.3567' as decimal) as add_res7
                    , cast('200' as smallint) / cast('12.3567' as decimal)  as add_res8
                    , cast('200' as int) / cast('12.3567' as decimal) as add_res9
                    , cast('200' as bigint) / cast('12.3567' as decimal) as add_res10
                    , cast('200.001' as float) / cast('12.3567' as decimal) as add_res11
                    , cast('200.001' as double) / cast('12.3567' as decimal) as add_res12
                    , cast('200.001' as decimal) / cast('12.3567' as decimal) as add_res13
                    """)
    }

    @Test
    fun testDecimalV2LiteralModOtherLiteral() {
        query("""
                    select 
                      cast('12.3567' as decimal) % cast('200' as tinyint) as add_res0
                    , cast('12.3567' as decimal) % cast('200' as smallint) as add_res1
                    , cast('12.3567' as decimal) % cast('200' as int) as add_res2
                    , cast('12.3567' as decimal) % cast('200' as bigint) as add_res3
                    , cast('12.3567' as decimal) % cast('200.001' as float) as add_res4
                    , cast('12.3567' as decimal) % cast('200.001' as double) as add_res5
                    , cast('12.3567' as decimal) % cast('200.001' as decimal) as add_res6
                    , cast('200' as tinyint) % cast('12.3567' as decimal) as add_res7
                    , cast('200' as smallint) % cast('12.3567' as decimal)  as add_res8
                    , cast('200' as int) % cast('12.3567' as decimal) as add_res9
                    , cast('200' as bigint) % cast('12.3567' as decimal) as add_res10
                    , cast('200.001' as float) % cast('12.3567' as decimal) as add_res11
                    , cast('200.001' as double) % cast('12.3567' as decimal) as add_res12
                    , cast('200.001' as decimal) % cast('12.3567' as decimal) as add_res13
                    """)
    }

    @Test
    fun testDecimalLiteralModOtherLiteral() {
        query("""
                    select 
                      cast('12.3567' as decimal32(9,4)) % cast('200' as tinyint) as add_res0
                    , cast('12.3567' as decimal32(9,4)) % cast('200' as smallint) as add_res1
                    , cast('12.3567' as decimal32(9,4)) % cast('200' as int) as add_res2
                    , cast('12.3567' as decimal32(9,4)) % cast('200' as bigint) as add_res3
                    , cast('12.3567' as decimal32(9,4)) % cast('200.001' as float) as add_res4
                    , cast('12.3567' as decimal32(9,4)) % cast('200.001' as double) as add_res5
                    , cast('12.3567' as decimal32(9,4)) % cast('200.001' as decimal) as add_res6
                    , cast('200' as tinyint) % cast('12.3567' as decimal32(9,4)) as add_res7
                    , cast('200' as smallint) % cast('12.3567' as decimal32(9,4))  as add_res8
                    , cast('200' as int) % cast('12.3567' as decimal32(9,4)) as add_res9
                    , cast('200' as bigint) % cast('12.3567' as decimal32(9,4)) as add_res10
                    , cast('200.001' as float) % cast('12.3567' as decimal32(9,4)) as add_res11
                    , cast('200.001' as double) % cast('12.3567' as decimal32(9,4)) as add_res12
                    , cast('200.001' as decimal) % cast('12.3567' as decimal32(9,4)) as add_res13
                    """)
    }

    @Test
    fun testDecimalV2AndLiteralOp(){
        query("""
                    select 
                      col_nullable_decimalv2 + 123.45678 as res1
                    , col_nullable_decimalv2 - 124.45678 as res2
                    , col_nullable_decimalv2 * 124.45678 as res3
                    , col_nullable_decimalv2 / 124.45678 as res4
                    , col_nullable_decimalv2 % 124.45678 as res5
                    , 123.45678 + col_nullable_decimalv2 as res6
                    , 123.45678 - col_nullable_decimalv2 as res7
                    , 123.45678 * col_nullable_decimalv2 as res8
                    , 123.45678 / col_nullable_decimalv2 as res9
                    , 123.45678 % col_nullable_decimalv2 as res10
                    from $table
                    limit 10
                    """)
    }

    @Test
    fun testDecimalV2AndDecimal32Op(){
        query("""
                    select 
                      col_nullable_decimalv2 + col0_nullable_decimal_p9s2 as res1
                    , col_nullable_decimalv2 - col0_nullable_decimal_p9s2 as res2
                    , col_nullable_decimalv2 * col0_nullable_decimal_p9s2 as res3
                    , col_nullable_decimalv2 / col0_nullable_decimal_p9s2 as res4
                    , col_nullable_decimalv2 % col0_nullable_decimal_p9s2 as res5
                    , col0_nullable_decimal_p9s2 + col_nullable_decimalv2 as res6
                    , col0_nullable_decimal_p9s2 - col_nullable_decimalv2 as res7
                    , col0_nullable_decimal_p9s2 * col_nullable_decimalv2 as res8
                    , col0_nullable_decimal_p9s2 / col_nullable_decimalv2 as res9
                    , col0_nullable_decimal_p9s2 % col_nullable_decimalv2 as res10
                    from $table
                    limit 40
                    """)
    }

    @Test
    fun testDecimalV2AndDecimal64Op(){
        query("""
                    select 
                      col_nullable_decimalv2 + col0_nullable_decimal_p15s3 as res1
                    , col_nullable_decimalv2 - col0_nullable_decimal_p15s3 as res2
                    , col_nullable_decimalv2 * col0_nullable_decimal_p15s3 as res3
                    , col_nullable_decimalv2 / col0_nullable_decimal_p15s3 as res4
                    , col_nullable_decimalv2 % col0_nullable_decimal_p15s3 as res5
                    , col0_nullable_decimal_p15s3 + col_nullable_decimalv2 as res6
                    , col0_nullable_decimal_p15s3 - col_nullable_decimalv2 as res7
                    , col0_nullable_decimal_p15s3 * col_nullable_decimalv2 as res8
                    , col0_nullable_decimal_p15s3 / col_nullable_decimalv2 as res9
                    , col0_nullable_decimal_p15s3 % col_nullable_decimalv2 as res10
                    from $table
                    limit 40
                    """)
    }

    @Test
    fun testDecimalV2AndDecimal128Op(){
        query("""
                    select 
                      col_nullable_decimalv2 + col0_nullable_decimal_p38s6 as res1
                    , col_nullable_decimalv2 - col0_nullable_decimal_p38s6 as res2
                    , col_nullable_decimalv2 * col0_nullable_decimal_p38s6 as res3
                    , col_nullable_decimalv2 / col0_nullable_decimal_p38s6 as res4
                    , col_nullable_decimalv2 % col0_nullable_decimal_p38s6 as res5
                    , col0_nullable_decimal_p38s6 + col_nullable_decimalv2 as res6
                    , col0_nullable_decimal_p38s6 - col_nullable_decimalv2 as res7
                    , col0_nullable_decimal_p38s6 * col_nullable_decimalv2 as res8
                    , col0_nullable_decimal_p38s6 / col_nullable_decimalv2 as res9
                    , col0_nullable_decimal_p38s6 % col_nullable_decimalv2 as res10
                    from $table
                    limit 40
                    """)
    }
    @Test
    fun testDecimalMoneyFormat(){
        query(
                """
                    select
                        1
                        , col0_nullable_decimal_p9s2 as d0
                        , money_format(col0_nullable_decimal_p9s2) as res0
                        , col_nullable_decimal_p9s6 as d1
                        , money_format(col_nullable_decimal_p9s6) as res1
                        , col_nullable_decimal_p9s9 as d2
                        , money_format(col_nullable_decimal_p9s9) as res2
                        , col_nullable_decimal_p15s1 as d3
                        , money_format(col_nullable_decimal_p15s1) as res3
                        , col_nullable_decimal_p18s15 as d4
                        , money_format(col_nullable_decimal_p18s15) as res4
                        , col_nullable_decimal_p38s1 as d5
                        , money_format(col_nullable_decimal_p38s1) as res5
                        , col_nullable_decimal_p38s2 as d6
                        , money_format(col_nullable_decimal_p38s2) as res6
                        , col_nullable_decimal_p38s38 as d7
                        , money_format(col_nullable_decimal_p38s38) as res7
                    from $table 
                    limit 40
                """.trimIndent()
        )
    }

    @Test
    fun testDecimalAbs(){
        query(
                """
                    select
                        1
                        , col0_nullable_decimal_p9s2 as d0
                        , abs(col0_nullable_decimal_p9s2) as res0
                        , col_nullable_decimal_p9s6 as d1
                        , abs(col_nullable_decimal_p9s6) as res1
                        , col_nullable_decimal_p9s9 as d2
                        , abs(col_nullable_decimal_p9s9) as res2
                        , col_nullable_decimal_p15s1 as d3
                        , abs(col_nullable_decimal_p15s1) as res3
                        , col_nullable_decimal_p18s15 as d4
                        , abs(col_nullable_decimal_p18s15) as res4
                        , col_nullable_decimal_p38s1 as d5
                        , abs(col_nullable_decimal_p38s1) as res5
                        , col_nullable_decimal_p38s2 as d6
                        , abs(col_nullable_decimal_p38s2) as res6
                        , col_nullable_decimal_p38s38 as d7
                        , abs(col_nullable_decimal_p38s38) as res7
                    from $table 
                    limit 40
                """.trimIndent()
        )
    }

    @Test
    fun testDecimalNegative(){
        query(
                """
                    select
                        1
                        , col0_nullable_decimal_p9s2 as d0
                        , negative(col0_nullable_decimal_p9s2) as res0
                        , col_nullable_decimal_p9s6 as d1
                        , negative(col_nullable_decimal_p9s6) as res1
                        , col_nullable_decimal_p9s9 as d2
                        , negative(col_nullable_decimal_p9s9) as res2
                        , col_nullable_decimal_p15s1 as d3
                        , negative(col_nullable_decimal_p15s1) as res3
                        , col_nullable_decimal_p18s15 as d4
                        , negative(col_nullable_decimal_p18s15) as res4
                        , col_nullable_decimal_p38s1 as d5
                        , negative(col_nullable_decimal_p38s1) as res5
                        , col_nullable_decimal_p38s2 as d6
                        , negative(col_nullable_decimal_p38s2) as res6
                        , col_nullable_decimal_p38s38 as d7
                        , negative(col_nullable_decimal_p38s38) as res7
                    from $table 
                    limit 40
                """.trimIndent()
        )
    }

    @Test
    fun testDecimalPositive(){
        query(
                """
                    select
                        1
                        , col0_nullable_decimal_p9s2 as d0
                        , positive(col0_nullable_decimal_p9s2) as res0
                        , col_nullable_decimal_p9s6 as d1
                        , positive(col_nullable_decimal_p9s6) as res1
                        , col_nullable_decimal_p9s9 as d2
                        , positive(col_nullable_decimal_p9s9) as res2
                        , col_nullable_decimal_p15s1 as d3
                        , positive(col_nullable_decimal_p15s1) as res3
                        , col_nullable_decimal_p18s15 as d4
                        , positive(col_nullable_decimal_p18s15) as res4
                        , col_nullable_decimal_p38s1 as d5
                        , positive(col_nullable_decimal_p38s1) as res5
                        , col_nullable_decimal_p38s2 as d6
                        , positive(col_nullable_decimal_p38s2) as res6
                        , col_nullable_decimal_p38s38 as d7
                        , positive(col_nullable_decimal_p38s38) as res7
                    from $table 
                    limit 40
                """.trimIndent()
        )
    }

    @Test
    fun testDecimalModConst(){
        query(
                """
                    select
                        1
                        , col0_nullable_decimal_p9s2
                        , col0_nullable_decimal_p9s2 % 3.1415 as d0
                        , mod(col0_nullable_decimal_p9s2, 3.1415) as res0
                        , col_nullable_decimal_p9s6 % 30.1415 as d1
                        , mod(col_nullable_decimal_p9s6, 30.1415) as res1
                        , col_nullable_decimal_p9s9 % 99.434  as d2
                        , mod(col_nullable_decimal_p9s9,99.434) as res2
                        , col_nullable_decimal_p15s1 % 20 as d3
                        , mod(col_nullable_decimal_p15s1,20) as res3
                        , col_nullable_decimal_p18s15 % 0.56  as d4
                        , mod(col_nullable_decimal_p18s15,0.56) as res4
                        , col_nullable_decimal_p38s1 % 41 as d5
                        , mod(col_nullable_decimal_p38s1, 41) as res5
                        , col_nullable_decimal_p38s2 % 56 as d6
                        , mod(col_nullable_decimal_p38s2,56) as res6
                        , col_nullable_decimal_p38s38 % 2323 as d7
                        , mod(col_nullable_decimal_p38s38, 2323) as res7
                    from $table 
                    limit 40
                """.trimIndent()
        )
    }

    @Test
    fun testDecimalLeastAndGreatest(){
        query(
                """
                    select
                        1
                        , least(NULL, NULL, NULL) as res0
                        , greatest(NULL, NULL, NULL) as res1
                        , least(NULL, cast(111111.33 as DECIMAL32(9,2))) as res2
                        , greatest(NULL, cast(111111.33 as DECIMAL32(9,2))) as res3
                        , least(NULL, cast(111111.33 as DECIMAL32(9,2)), cast(99999999.1 as DECIMAL32(9,1))) as res4
                        , greatest(NULL, cast(111111.33 as DECIMAL32(9,2)), cast(99999999.1 as DECIMAL32(9,1))) as res5
                        , least(cast(111111.33 as DECIMAL32(9,2)), cast(99999999.1 as DECIMAL32(9,1))) as res6
                        , greatest(cast(111111.33 as DECIMAL32(9,2)), cast(99999999.1 as DECIMAL32(9,1))) as res7
                        , col_decimal_p9s0, col_decimal_p9s1, col_decimal_p18s0, col_decimal_p18s1, col_decimal_p38s0, col_decimal_p38s1
                        , least(col_decimal_p9s0, col_decimal_p9s1, col_decimal_p18s0, col_decimal_p18s1, col_decimal_p38s0, col_decimal_p38s1) as least_result
                        , greatest(col_decimal_p9s0, col_decimal_p9s1, col_decimal_p18s0, col_decimal_p18s1, col_decimal_p38s0, col_decimal_p38s1) as greatest_result
                    from $table 
                    limit 40
                """.trimIndent()
        )
    }

    @Test
    fun testDecimalDecimalIfConst() {
        query(
                """
                    select
                      1
                      , if(1, cast('3.14' as decimal32(9,2)), cast('1.9999' as decimal32(5,4))) as res0
                      , if(0, cast('3.14' as decimal32(9,2)), cast('1.9999' as decimal32(5,4))) as res1
                      , if(NULL, cast('3.14' as decimal32(9,2)), cast('1.9999' as decimal32(5,4))) as res2
                      , if(-1, cast('3.14' as decimal32(9,2)), cast('1.9999' as decimal32(5,4))) as res3
                      
                      , if(1, cast('3.14' as decimal64(9,2)), cast('1.9999' as decimal64(5,4))) as res4
                      , if(0, cast('3.14' as decimal64(9,2)), cast('1.9999' as decimal64(5,4))) as res5
                      , if(NULL, cast('3.14' as decimal64(9,2)), cast('1.9999' as decimal64(5,4))) as res6
                      , if(-1, cast('3.14' as decimal64(9,2)), cast('1.9999' as decimal64(5,4))) as res7
                      
                      , if(1, cast('3.14' as decimal128(9,2)), cast('1.9999' as decimal128(5,4))) as res9
                      , if(0, cast('3.14' as decimal128(9,2)), cast('1.9999' as decimal128(5,4))) as res10
                      , if(NULL, cast('3.14' as decimal128(9,2)), cast('1.9999' as decimal128(5,4))) as res11
                      , if(-1, cast('3.14' as decimal128(9,2)), cast('1.9999' as decimal128(5,4))) as res12
                      
              """.trimIndent())
    }

    @Test
    fun testDecimalDecimalIf() {
        query(
                """
                    select
                      1
                      , col0_decimal_p9s2, if(col0_decimal_p9s2 >= 0, col0_decimal_p9s2, - col0_decimal_p9s2) as res0
                      , col_decimal_p18s18, if(col_decimal_p18s18 >= 0, col_decimal_p18s18, - col_decimal_p18s18) as res1
                      , col_decimal_p38s38, if(col_decimal_p38s38 >= 0, col_decimal_p38s38, - col_decimal_p38s38) as res2
                      , col_nullable_decimal_p9s9, col_nullable_decimal_p9s9 is NULL as is_null, if(col_nullable_decimal_p9s9 is NULL, 0, col_nullable_decimal_p9s9) as res3
                      , col_nullable_decimal_p15s10, col_nullable_decimal_p15s10 is NULL as is_null, if(col_nullable_decimal_p15s10 is NULL, 0, col_nullable_decimal_p15s10) as res4
                      , col_nullable_decimal_p33s17, col_nullable_decimal_p33s17 is NULL as is_null, if(col_nullable_decimal_p33s17 is NULL, 0, col_nullable_decimal_p33s17) as res5
                    from $table
                    limit 10;
              """.trimIndent())
    }

    @Test
    fun testDecimalDecimalIfNull() {
        query(
                """
                    select
                      1
                      , col0_decimal_p9s2, ifnull(0, col0_decimal_p9s2) as res0
                      , col_decimal_p18s18, ifnull(null, col_decimal_p18s18) as res1
                      , col_decimal_p38s38, ifnull(1, col_decimal_p38s38) as res2
                      , col_nullable_decimal_p9s9, col_nullable_decimal_p9s9 is NULL as is_null, ifnull(col_nullable_decimal_p9s9, 3.14) as res3
                      , col_nullable_decimal_p15s10, col_nullable_decimal_p15s10 is NULL as is_null, ifnull(col_nullable_decimal_p15s10, 3.14) as res4
                      , col_nullable_decimal_p33s17, col_nullable_decimal_p33s17 is NULL as is_null, ifnull(col_nullable_decimal_p33s17, 3.14) as res5
                    from $table
                    limit 10;
              """.trimIndent())
    }

    @Test
    fun testDecimalDecimalNullIf() {
        query(
                """
                    select
                      1
                      , col0_decimal_p9s2, nullif(cast('-589962.71' as decimal32(9,3)), col0_decimal_p9s2) as res0
                      , col_decimal_p18s18, nullif(col_decimal_p18s18, cast(1E-18 as decimal64(18,18))) as res1
                      , col_decimal_p38s38, nullif(cast('-0.99999999999999999999999039773745784102' as decimal128(38,38)), col_decimal_p38s38) as res2
                      , col_nullable_decimal_p9s9, col_nullable_decimal_p9s9 is NULL as is_null, nullif(col_nullable_decimal_p9s9, cast('-47474.9023850552' as decimal32(9,9))) as res3
                      , col_nullable_decimal_p15s10, col_nullable_decimal_p15s10 is NULL as is_null, nullif(col_nullable_decimal_p15s10, null) as res4
                      , col_nullable_decimal_p33s17, col_nullable_decimal_p33s17 is NULL as is_null, nullif(null, col_nullable_decimal_p33s17) as res5
                    from $table
                    limit 10;
              """.trimIndent())
    }

    @Test
    fun testDecimalDecimalCoalesce() {
        query(
                """
                    select
                      1
                      , coalesce(cast('-589962.71' as decimal32(9,3)), col0_decimal_p9s2) as res0
                      , coalesce(col_decimal_p18s18, cast(1E-18 as decimal64(18,18))) as res1
                      , coalesce(cast('-0.99999999999999999999999039773745784102' as decimal128(38,38)), col_decimal_p38s38) as res2
                      , col_nullable_decimal_p9s9, coalesce(col_nullable_decimal_p9s9, null, cast('-47474.9023850552' as decimal32(9,9))) as res3
                      , col_nullable_decimal_p15s10, coalesce(col_nullable_decimal_p15s10, null, null, 3.14) as res4
                      , col_nullable_decimal_p33s17, coalesce(null, col_nullable_decimal_p33s17, null,null,null,null) as res5
                    from $table
                    limit 10;
              """.trimIndent())
    }

    @Test
    fun testDecimal32p9s2Aggregation(){
        query(
                """
                    select
                        1
                        , max(col0_decimal_p9s2) as max
                        , min(col0_decimal_p9s2) as min
                        , sum(col0_decimal_p9s2) as sum
                        , count(col0_decimal_p9s2) as count
                        , avg(col0_decimal_p9s2) as avg
                        , ndv(col0_decimal_p9s2) as ndv
                        , stddev(col0_decimal_p9s2) as stddev
                        , stddev_samp(col0_decimal_p9s2) as stddev_samp
                        , variance(col0_decimal_p9s2) as variance
                        , variance_pop(col0_decimal_p9s2) as variance_pop
                        , var_pop(col0_decimal_p9s2) as var_pop
                        , variance_samp(col0_decimal_p9s2) as variance_samp
                        , var_samp(col0_decimal_p9s2) as var_samp
                    from $table 
                """.trimIndent()
        )
    }

    @Test
    fun testNullableDecimal32p9s6Aggregation(){
        query(
                """
                    select
                        1
                        , max(col_nullable_decimal_p9s6) as max
                        , min(col_nullable_decimal_p9s6) as min
                        , sum(col_nullable_decimal_p9s6) as sum
                        , count(col_nullable_decimal_p9s6) as count
                        , avg(col_nullable_decimal_p9s6) as avg
                        , ndv(col_nullable_decimal_p9s6) as ndv
                        , stddev(col_nullable_decimal_p9s6) as stddev
                        , stddev_samp(col_nullable_decimal_p9s6) as stddev_samp
                        , variance(col_nullable_decimal_p9s6) as variance
                        , variance_pop(col_nullable_decimal_p9s6) as variance_pop
                        , var_pop(col_nullable_decimal_p9s6) as var_pop
                        , variance_samp(col_nullable_decimal_p9s6) as variance_samp
                        , var_samp(col_nullable_decimal_p9s6) as var_samp
                    from $table 
                """.trimIndent()
        )
    }

    @Test
    fun testDecimal64p15s3Aggregation(){
        query(
                """
                    select
                        1
                        , max(col0_decimal_p15s3) as max
                        , min(col0_decimal_p15s3) as min
                        , sum(col0_decimal_p15s3) as sum
                        , count(col0_decimal_p15s3) as count
                        , avg(col0_decimal_p15s3) as avg
                        , ndv(col0_decimal_p15s3) as ndv
                        , stddev(col0_decimal_p15s3) as stddev
                        , stddev_samp(col0_decimal_p15s3) as stddev_samp
                        , variance(col0_decimal_p15s3) as variance
                        , variance_pop(col0_decimal_p15s3) as variance_pop
                        , var_pop(col0_decimal_p15s3) as var_pop
                        , variance_samp(col0_decimal_p15s3) as variance_samp
                        , var_samp(col0_decimal_p15s3) as var_samp
                    from $table 
                """.trimIndent()
        )
    }

    @Test
    fun testNullableDecimal64p15s15Aggregation(){
        query(
                """
                    select
                        1
                        , max(col_nullable_decimal_p15s15) as max
                        , min(col_nullable_decimal_p15s15) as min
                        , sum(col_nullable_decimal_p15s15) as sum
                        , count(col_nullable_decimal_p15s15) as count
                        , avg(col_nullable_decimal_p15s15) as avg
                        , ndv(col_nullable_decimal_p15s15) as ndv
                        , stddev(col_nullable_decimal_p15s15) as stddev
                        , stddev_samp(col_nullable_decimal_p15s15) as stddev_samp
                        , variance(col_nullable_decimal_p15s15) as variance
                        , variance_pop(col_nullable_decimal_p15s15) as variance_pop
                        , variance_samp(col_nullable_decimal_p15s15) as variance_samp
                        , var_samp(col_nullable_decimal_p15s15) as var_samp
                    from $table 
                """.trimIndent()
        )
    }

    @Test
    fun testDecimal128p38s33Aggregation(){
        query(
                """
                    select
                        1
                        , max(col_decimal_p38s33) as max
                        , min(col_decimal_p38s33) as min
                        , sum(col_decimal_p38s33) as sum
                        , count(col_decimal_p38s33) as count
                        , avg(col_decimal_p38s33) as avg
                        , ndv(col_decimal_p38s33) as ndv
                        , stddev(col_decimal_p38s33) as stddev
                        , stddev_samp(col_decimal_p38s33) as stddev_samp
                        , variance(col_decimal_p38s33) as variance
                        , variance_pop(col_decimal_p38s33) as variance_pop
                        , variance_samp(col_decimal_p38s33) as variance_samp
                        , var_samp(col_decimal_p38s33) as var_samp
                    from $table 
                """.trimIndent()
        )
    }

    @Test
    fun testNullableDecimal128p33s17Aggregation(){
        query(
                """
                    select
                        1
                        , max(col_nullable_decimal_p33s17) as max
                        , min(col_nullable_decimal_p33s17) as min
                        , sum(col_nullable_decimal_p33s17) as sum
                        , count(col_nullable_decimal_p33s17) as count
                        , avg(col_nullable_decimal_p33s17) as avg
                        , ndv(col_nullable_decimal_p33s17) as ndv
                        , stddev(col_nullable_decimal_p33s17) as stddev
                        , stddev_samp(col_nullable_decimal_p33s17) as stddev_samp
                        , variance(col_nullable_decimal_p33s17) as variance
                        , variance_pop(col_nullable_decimal_p33s17) as variance_pop
                        , variance_samp(col_nullable_decimal_p33s17) as variance_samp
                        , var_samp(col_nullable_decimal_p33s17) as var_samp
                    from $table 
                """.trimIndent()
        )
    }

    @Test
    fun testDecimalAggregationMax_decimal32p9s9(){
        query(
                """
                    select
                          1
                        , max(col_decimal_p9s9, col_decimal_p9s1) as max
                    from $table
                """.trimIndent()
        )
    }

    @Test
    fun testDecimalAggregationPercentileApprox(){
        query(
                """
                    select
                          1
                        , max(col_decimal_p9s9)
                        /*, max(cast(col_decimal_p9s9 as double)) as max_2*/
                        /*
                        , percentile_approx(col_decimal_p9s9, 1.00, 5000) as p100_1
                        , percentile_approx(cast(col_decimal_p9s9 as double), 1.00, 5000) as p100_2
                        */
                    from $table
                """.trimIndent()
        )
    }

    @Test
    fun testDecimalV2AndFloat() {
        query(
                """
                    select col_decimalv2, col_float, col_decimalv2 + col_float as result
                    from $table
                    limit 10;
                """.trimIndent()
        )
    }

    @Test
    fun testDecimalV2AndInteger() {
        query(
                """
                    select 
                        col_decimalv2
                        , col_largeint, col_decimalv2 + col_largeint as result0
                        , col_smallint, col_decimalv2 + col_smallint as result1
                    from $table
                    limit 10;
                """.trimIndent()
        )
    }

    @Test
    fun testDecimal32SubConstFloat() {
        query("""
                    select 
                        col0_decimal_p9s2 as a
                        ,col0_decimal_p9s2 - cast(3.14 as decimal32(9,2)) as c0
                        ,col0_decimal_p9s2 - 3.14 as c1
                        ,(0-(3.14 - col0_decimal_p9s2)) as c2
                        ,(0-(cast('3.14' as decimal32(9,2)) - col0_decimal_p9s2)) as c3
                        ,(0-(cast('3.14' as float) - col0_decimal_p9s2)) as c4
                        ,(0-(cast('3.14' as double) - col0_decimal_p9s2)) as c5
                        ,(0-(cast(cast('3.14' as float) as decimal32(9,2)) - col0_decimal_p9s2)) as c6
                        ,(0-(cast(cast('3.14' as double) as decimal32(9,2)) - col0_decimal_p9s2)) as c7
                        ,(0-(cast(cast('3.14' as decimal) as decimal32(9,2)) - col0_decimal_p9s2)) as c8
                        ,(0-(cast('3.14' as decimal64(18,2)) - col0_decimal_p9s2)) as c9
                        ,(0-(cast('3.14' as decimal128(38,2)) - col0_decimal_p9s2)) as c10
                        ,(0-(cast('3.14' as decimal128(38,29)) - col0_decimal_p9s2)) as c11
                    from $table 
                    limit 10;
                    """)
    }

    @Test
    fun testDecimal32MulConstFloat() {
        query("""
                    select 
                        col0_decimal_p9s2 as a
                        ,col0_decimal_p9s2 * cast(3.14 as decimal32(9,2)) as c0
                        ,col0_decimal_p9s2 * 3.14 as c1
                        ,3.14 * col0_decimal_p9s2 as c2
                        ,cast('3.14' as decimal32(9,2)) * col0_decimal_p9s2 as c3
                        ,cast('3.14' as float) * col0_decimal_p9s2 as c4
                        ,cast('3.14' as double) * col0_decimal_p9s2 as c5
                        ,cast(cast('3.14' as float) as decimal32(9,2)) * col0_decimal_p9s2 as c6
                        ,cast(cast('3.14' as double) as decimal32(9,2)) * col0_decimal_p9s2 as c7
                        ,cast(cast('3.14' as decimal) as decimal32(9,2)) * col0_decimal_p9s2 as c8
                        ,cast('3.14' as decimal64(18,2)) * col0_decimal_p9s2 as c9
                        ,cast('3.14' as decimal128(38,2)) * col0_decimal_p9s2 as c10
                        ,cast('3.14' as decimal128(38,29)) * col0_decimal_p9s2 as c11
                    from $table 
                    limit 10;
                    """)
    }

    @Test
    fun testDecimal32DivConstFloat() {
        query("""
                    select 
                        col0_decimal_p9s2 as a
                        ,col0_decimal_p9s2 / cast(3.14 as decimal32(9,2)) as c0
                        ,col0_decimal_p9s2 / 3.14 as c1
                        ,3.14 / col0_decimal_p9s2 as c2
                        ,cast('3.14' as decimal32(9,2)) / col0_decimal_p9s2 as c3
                        ,cast('3.14' as float) / col0_decimal_p9s2 as c4
                        ,cast('3.14' as double) / col0_decimal_p9s2 as c5
                        ,cast(cast('3.14' as float) as decimal32(9,2)) / col0_decimal_p9s2 as c6
                        ,cast(cast('3.14' as double) as decimal32(9,2)) / col0_decimal_p9s2 as c7
                        ,cast(cast('3.14' as decimal) as decimal32(9,2))/ col0_decimal_p9s2 as c8
                        ,cast('3.14' as decimal64(18,2)) / col0_decimal_p9s2 as c9
                        ,cast('3.14' as decimal128(38,2)) / col0_decimal_p9s2 as c10
                        ,cast('3.14' as decimal128(38,29)) / col0_decimal_p9s2 as c11
                    from $table 
                    limit 10;
                    """)
    }

    @Test
    fun testDecimal32ModConstFloat() {
        query("""
                    select 
                        col0_decimal_p9s2 as a
                        ,col0_decimal_p9s2 % cast(3.14 as decimal32(9,2)) as c0
                        ,col0_decimal_p9s2 % 3.14 as c1
                        ,3.14 % col0_decimal_p9s2 as c2
                        ,cast('3.14' as decimal32(9,2)) % col0_decimal_p9s2 as c3
                        ,cast('3.14' as float) % col0_decimal_p9s2 as c4
                        ,cast('3.14' as double) % col0_decimal_p9s2 as c5
                        ,cast(cast('3.14' as float) as decimal32(9,2)) % col0_decimal_p9s2 as c6
                        ,cast(cast('3.14' as double) as decimal32(9,2)) % col0_decimal_p9s2 as c7
                        ,cast(cast('3.14' as decimal) as decimal32(9,2))% col0_decimal_p9s2 as c8
                        ,cast('3.14' as decimal64(18,2)) % col0_decimal_p9s2 as c9
                        ,cast('3.14' as decimal128(38,2)) % col0_decimal_p9s2 as c10
                        ,cast('3.14' as decimal128(38,29)) % col0_decimal_p9s2 as c11
                    from $table 
                    limit 10;
                    """)
    }


    @Test
    fun testDecimal32AddConstFloatSimple() {
        query("""
                    select 
                        col0_decimal_p9s2 as a
                        ,(cast("3.14" as decimal32(9,2)) + col0_decimal_p9s2) as c3
                    from $table 
                    limit 10;
                    """)
    }

    @Test
    fun testDecimal32AddFloat() {
        query("""
                    select 
                        col0_decimal_p9s2 as a, 
                        col_float as b,
                        cast(col_float as decimal64(18,6)) as decimal_b,
                        col0_decimal_p9s2 + col_float as c
                    from $table 
                    limit 10;
                    """)
    }

    @Test
    fun testDecimal32Sub() {
        query("""
                    select 
                        col0_decimal_p9s2 as a, 
                        col1_decimal_p9s2 as b, 
                        col0_decimal_p9s2 - col1_decimal_p9s2  as c
                    from $table 
                    limit 10;
                    """)
    }

    @Test
    fun testDecimal32AddDifferentScale() {
        query("""
                    select 
                        col0_decimal_p9s2 as a,
                        col_decimal_p9s8 as b,
                        col0_decimal_p9s2 + col_decimal_p9s8  as c
                    from $table 
                    limit 10;
                    """)
    }

    @Test
    fun testDecimal32SubDifferentScale() {
        query("""
                    select 
                        col0_decimal_p9s2 as a,
                        col_decimal_p9s8 as b,
                        col0_decimal_p9s2 - col_decimal_p9s8  as c
                    from $table 
                    limit 10;
                    """)
    }

    @Test
    fun testDecimal32AddDecimal128SameScale() {
        query("""
                    select 
                        col_decimal_p9s1 as a,
                        col_decimal_p38s1 as b,
                        col_decimal_p9s1 + col_decimal_p38s1  as c
                    from $table 
                    limit 10;
                    """)
    }

    @Test
    fun testDecimal32SubDecimal128SameScale() {
        query("""
                    select 
                        col_decimal_p9s1 as a,
                        col_decimal_p38s1 as b,
                        col_decimal_p9s1 - col_decimal_p38s1  as c
                    from $table 
                    limit 10;
                    """)
    }

    @Test
    fun testDecimal32AddDecimal128DifferentScale() {
        query("""
                    select 
                        col_decimal_p9s9 as a,
                        col_decimal_p38s33 as b,
                        col_decimal_p9s9 + col_decimal_p38s33  as c
                    from $table 
                    limit 10;
                    """)
    }

    @Test
    fun testDecimal32SubDecimal128DifferentScale() {
        query("""
                    select 
                        col_decimal_p9s9 as a,
                        col_decimal_p38s33 as b,
                        col_decimal_p9s9 - col_decimal_p38s33  as c
                    from $table 
                    limit 10;
                    """)
    }

    @Test
    fun testDecimal32MulDecimal32() {
        query("""
                    select 
                        col0_decimal_p9s2 as a,
                        col0_decimal_p9s2 as b,
                        col0_decimal_p9s2 * col0_decimal_p9s2  as c
                    from $table 
                    limit 10;
                    """)
    }

    @Test
    fun testDecimal32DivDecimal32() {
        query("""
                    select 
                        col0_decimal_p9s2 as a,
                        col1_decimal_p9s2 as b,
                        col0_decimal_p9s2 / col1_decimal_p9s2  as c
                    from $table 
                    limit 10;
                    """)
    }

    @Test
    fun testDecimal64MulDecimal64() {
        query("""
                    select 
                        col0_decimal_p15s6 as a,
                        col0_decimal_p15s3 as b,
                        col0_decimal_p15s6 * col0_decimal_p15s3  as c
                    from $table 
                    limit 10;
                    """)
    }

    @Test
    fun testDecimal64DivDecimal64() {
        query("""
                    select 
                        col0_decimal_p15s6 as a,
                        col0_decimal_p15s3 as b,
                        col0_decimal_p15s6 / col0_decimal_p15s3  as c
                    from $table 
                    limit 10;
                    """)
    }

    @Test
    fun testDecimal64MulDecimal128() {
        query("""
            select t.c, t.d
            from (
                    select 
                        col0_decimal_p38s3 as a,
                        col0_decimal_p38s6 as b,
                        col0_decimal_p38s6 * col0_decimal_p38s3  as c,
                        col0_decimal_p38s3 * col0_decimal_p38s6  as d
                    from $table 
                    limit 10
            ) as t;
               """)
    }

    @Test
    fun testDecimal64DivDecimal128() {
        query("""
            select a,b,t.c, t.d
            from (
                    select 
                        col0_decimal_p38s3 as a,
                        col0_decimal_p38s6 as b,
                        col0_decimal_p38s6 / col0_decimal_p38s3  as c,
                        col0_decimal_p38s3 / col0_decimal_p38s6  as d
                    from $table 
                    limit 10
            ) as t;
               """)
    }

    @Test
    fun testDecimal32ModDecimal32() {
        query("""
                    select 
                        col0_decimal_p9s2 as a,
                        col1_decimal_p9s2 as b,
                        col0_decimal_p9s2 % col1_decimal_p9s2  as c
                    from $table 
                    limit 10;
                    """)
    }


    @Test
    fun testDecimal64ModDecimal64() {
        query("""
                    select 
                        col0_decimal_p15s6 as a,
                        col0_decimal_p15s3 as b,
                        col0_decimal_p15s6 % col0_decimal_p15s3  as c
                    from $table 
                    limit 10;
                    """)
    }


    @Test
    fun testDecimal64ModDecimal128() {
        query("""
            select a,b,t.c, t.d
            from (
                    select 
                        col0_decimal_p38s3 as a,
                        col0_decimal_p38s6 as b,
                        col0_decimal_p38s6 % col0_decimal_p38s3  as c,
                        col0_decimal_p38s3 % col0_decimal_p38s6  as d
                    from $table 
                    limit 10
            ) as t;
               """)
    }

    @Test
    fun testDecimal32p9s2() {
        query("""
                    select 
                        col0_decimal_p9s2 as a
                    from $table 
                    limit 10
                    """)
    }

    @Test
    fun testReadOrcFile() {
        FunctionTestTable.readOrcFile("col_decimal_p9s0")
    }

    @Test
    fun testDecimal32CastAsDecimal64Add() {
        query("""
                    select 
                        cast(col0_decimal_p9s2 as Decimal64(9,2)) as a, 
                        cast(col0_decimal_p9s2 as Decimal64(9,2)) + cast(col0_decimal_p9s2 as Decimal64(9,2)) as c
                    from $table 
                    limit 10
                    """)
    }

    @Test
    fun testDecimal32CastAsDecimal64() {
        query("""
                    select 
                        cast(col0_decimal_p9s2 as Decimal64(18,4)) as a
                    from $table 
                    limit 10
                    """)
    }
}