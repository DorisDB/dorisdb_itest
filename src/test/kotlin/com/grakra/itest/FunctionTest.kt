package com.grakra.itest

import com.grakra.TestMethodCapture
import com.grakra.schema.*
import com.grakra.util.Util
import org.testng.Assert
import org.testng.annotations.AfterClass
import org.testng.annotations.BeforeClass
import org.testng.annotations.Listeners
import org.testng.annotations.Test
import java.io.File
import java.io.PrintStream

@Listeners(TestMethodCapture::class)
class FunctionTest : DorisDBRemoteITest() {

    val db = "decimal_test_db"
    val tableName = "decimal_all_types"

    val fields = listOf(
            SimpleField.fixedLength("key0", FixedLengthType.TYPE_INT),
            SimpleField.fixedLength("key1", FixedLengthType.TYPE_INT),
            SimpleField.fixedLength("key2", FixedLengthType.TYPE_INT),
            SimpleField.fixedLength("col_boolean", FixedLengthType.TYPE_BOOLEAN),
            SimpleField.fixedLength("col_tinyint", FixedLengthType.TYPE_TINYINT),
            SimpleField.fixedLength("col_smallint", FixedLengthType.TYPE_SMALLINT),
            SimpleField.fixedLength("col_int", FixedLengthType.TYPE_INT),
            SimpleField.fixedLength("col_bigint", FixedLengthType.TYPE_BIGINT),
            SimpleField.fixedLength("col_largeint", FixedLengthType.TYPE_LARGEINT),
            SimpleField.fixedLength("col_date", FixedLengthType.TYPE_DATE),
            SimpleField.fixedLength("col_timestamp", FixedLengthType.TYPE_DATETIME),
            SimpleField.fixedLength("col_float", FixedLengthType.TYPE_FLOAT),
            SimpleField.fixedLength("col_double", FixedLengthType.TYPE_DOUBLE),
            SimpleField.decimalv2("col_decimalv2", 27, 9),
            SimpleField.char("col_char", 20),
            SimpleField.varchar("col_varchar", 20),
            SimpleField.decimal("col_decimal_p9s0", 32, 9, 0),
            SimpleField.decimal("col_decimal_p9s1", 32, 9, 1),
            SimpleField.decimal("col0_decimal_p9s2", 32, 9, 2),
            SimpleField.decimal("col1_decimal_p9s2", 32, 9, 2),
            SimpleField.decimal("col_decimal_p9s8", 32, 9, 8),
            SimpleField.decimal("col_decimal_p9s9", 32, 9, 9),
            SimpleField.decimal("col_decimal_p18s0", 64, 18, 0),
            SimpleField.decimal("col_decimal_p18s1", 64, 18, 1),
            SimpleField.decimal("col0_decimal_p15s6", 64, 15, 6),
            SimpleField.decimal("col1_decimal_p15s6", 64, 15, 6),
            SimpleField.decimal("col0_decimal_p15s3", 64, 15, 3),
            SimpleField.decimal("col1_decimal_p15s3", 64, 15, 3),
            SimpleField.decimal("col_decimal_p15s15", 64, 15, 15),
            SimpleField.decimal("col_decimal_p18s18", 64, 18, 18),
            SimpleField.decimal("col_decimal_p38s0", 128, 38, 0),
            SimpleField.decimal("col_decimal_p38s1", 128, 38, 1),
            SimpleField.decimal("col0_decimal_p38s6", 128, 38, 6),
            SimpleField.decimal("col1_decimal_p38s6", 128, 38, 6),
            SimpleField.decimal("col0_decimal_p38s3", 128, 38, 3),
            SimpleField.decimal("col1_decimal_p38s3", 128, 38, 3),
            SimpleField.decimal("col_decimal_p38s33", 128, 38, 33),
            SimpleField.decimal("col_decimal_p38s38", 128, 38, 38),
            CompoundField.nullable(SimpleField.fixedLength("col_nullable_boolean", FixedLengthType.TYPE_BOOLEAN), 50),
            CompoundField.nullable(SimpleField.fixedLength("col_nullable_tinyint", FixedLengthType.TYPE_TINYINT), 50),
            CompoundField.nullable(SimpleField.fixedLength("col_nullable_smallint", FixedLengthType.TYPE_SMALLINT), 50),
            CompoundField.nullable(SimpleField.fixedLength("col_nullable_int", FixedLengthType.TYPE_INT), 50),
            CompoundField.nullable(SimpleField.fixedLength("col_nullable_bigint", FixedLengthType.TYPE_BIGINT), 50),
            CompoundField.nullable(SimpleField.fixedLength("col_nullable_largeint", FixedLengthType.TYPE_LARGEINT), 50),
            CompoundField.nullable(SimpleField.fixedLength("col_nullable_date", FixedLengthType.TYPE_DATE), 50),
            CompoundField.nullable(SimpleField.fixedLength("col_nullable_timestamp", FixedLengthType.TYPE_DATETIME), 50),
            CompoundField.nullable(SimpleField.fixedLength("col_nullable_float", FixedLengthType.TYPE_FLOAT), 50),
            CompoundField.nullable(SimpleField.fixedLength("col_nullable_double", FixedLengthType.TYPE_DOUBLE), 50),
            CompoundField.nullable(SimpleField.decimalv2("col_nullable_decimalv2", 27, 9), 50),
            CompoundField.nullable(SimpleField.char("col_nullable_char", 20), 50),
            CompoundField.nullable(SimpleField.varchar("col_nullable_varchar", 20), 50),
            CompoundField.nullable(SimpleField.decimal("col_nullable_decimal_p9s0", 32, 9, 0), 50),
            CompoundField.nullable(SimpleField.decimal("col_nullable_decimal_p9s1", 32, 9, 1), 50),
            CompoundField.nullable(SimpleField.decimal("col0_nullable_decimal_p9s2", 32, 9, 2), 50),
            CompoundField.nullable(SimpleField.decimal("col1_nullable_decimal_p9s2", 32, 9, 2), 50),
            CompoundField.nullable(SimpleField.decimal("col_nullable_decimal_p9s5", 32, 9, 5), 50),
            CompoundField.nullable(SimpleField.decimal("col_nullable_decimal_p9s6", 32, 9, 6), 50),
            CompoundField.nullable(SimpleField.decimal("col_nullable_decimal_p9s9", 32, 9, 9), 50),
            CompoundField.nullable(SimpleField.decimal("col0_nullable_decimal_p15s6", 64, 15, 6), 50),
            CompoundField.nullable(SimpleField.decimal("col1_nullable_decimal_p15s6", 64, 15, 6), 50),
            CompoundField.nullable(SimpleField.decimal("col0_nullable_decimal_p15s3", 64, 15, 3), 50),
            CompoundField.nullable(SimpleField.decimal("col1_nullable_decimal_p15s3", 64, 15, 3), 50),
            CompoundField.nullable(SimpleField.decimal("col_nullable_decimal_p15s0", 64, 15, 0), 50),
            CompoundField.nullable(SimpleField.decimal("col_nullable_decimal_p15s1", 64, 15, 1), 50),
            CompoundField.nullable(SimpleField.decimal("col_nullable_decimal_p15s10", 64, 15, 10), 50),
            CompoundField.nullable(SimpleField.decimal("col_nullable_decimal_p15s15", 64, 15, 15), 50),
            CompoundField.nullable(SimpleField.decimal("col_nullable_decimal_p18s15", 64, 18, 15), 50),
            CompoundField.nullable(SimpleField.decimal("col_nullable_decimal_p18s18", 64, 18, 18), 50),
            CompoundField.nullable(SimpleField.decimal("col0_nullable_decimal_p38s6", 128, 38, 6), 50),
            CompoundField.nullable(SimpleField.decimal("col1_nullable_decimal_p38s6", 128, 38, 6), 50),
            CompoundField.nullable(SimpleField.decimal("col0_nullable_decimal_p38s3", 128, 38, 3), 50),
            CompoundField.nullable(SimpleField.decimal("col1_nullable_decimal_p38s3", 128, 38, 3), 50),
            CompoundField.nullable(SimpleField.decimal("col_nullable_decimal_p38s0", 128, 38, 0), 50),
            CompoundField.nullable(SimpleField.decimal("col_nullable_decimal_p38s1", 128, 38, 1), 50),
            CompoundField.nullable(SimpleField.decimal("col_nullable_decimal_p38s2", 128, 38, 2), 50),
            CompoundField.nullable(SimpleField.decimal("col_nullable_decimal_p38s3", 128, 38, 3), 50),
            CompoundField.nullable(SimpleField.decimal("col_nullable_decimal_p33s17", 128, 33, 17), 50),
            CompoundField.nullable(SimpleField.decimal("col_nullable_decimal_p35s19", 128, 35, 19), 50),
            CompoundField.nullable(SimpleField.decimal("col_nullable_decimal_p38s30", 128, 38, 30), 50),
            CompoundField.nullable(SimpleField.decimal("col_nullable_decimal_p38s38", 128, 38, 38), 50))

    val table = Table(tableName, fields, 3)

    fun prepare_data() {
        OrcUtil.createOrcFile("decimal_all_types.orc", table.keyFields(), table.valueFields(emptySet()), 4097, 4096)
    }

    fun test_create_table() {
        create_db(db)
        create_table(db, table)
    }

    fun test_broker_load() {
        val loadSql = table.brokerLoadSql(db, "orc", "/user/decimal_v3_function_test/decimal_all_types.orc")
        admin_set_vectorized_load_enable(true)
        broker_load(loadSql)
    }

    val genCodeFile = "decimal_function.py"
    var genCode: PrintStream? = null
    val createSqlPrefix = "decimal_function/sql"
    val dataCsvPrefix = "decimal_function/csv"

    @BeforeClass
    fun setUp() {
        val genCodeFileObj = File(genCodeFile)
        if (genCodeFileObj.exists()) {
            genCodeFileObj.delete()
        }
        genCode = PrintStream(genCodeFileObj.outputStream())
        Util.recreateDir(createSqlPrefix)
        Util.recreateDir(dataCsvPrefix)
    }

    @AfterClass
    fun tearDown() {
        genCode!!.flush()
        genCode!!.close()
    }

    fun generate(test_name: String, stmt: String) {
        val fpSql = fingerprint_murmur_hash3_32_sql(db, stmt)
        val fpSqlPath = "$createSqlPrefix/$test_name.sql"
        Util.createFile(File(fpSqlPath), fpSql)
        val fp = fingerprint_murmur_hash3_32(db, stmt)
        val case = Util.renderTemplate(
                "decimal_function.template",
                "name" to test_name,
                "fp" to fp
        )

        genCode!!.println(case)

        run_mysql { c ->
            c.q(db) { sql ->
                sql.e("set enable_decimal_v3 = true")
                val resutl = sql.q(stmt)
                resutl!!.forEach { rows ->
                    rows.entries.joinToString { (k, v) -> "$k=$v" }.let { println(it) }
                }
            }
        }
    }

    @Test
    fun testDecimal32AddConstInt() {
        generate("decimal32_and_const_int", """
                    select 
                        col0_decimal_p9s2 as a, 
                        col0_decimal_p9s2 + 10  as c
                    from $tableName 
                    limit 10
                    """)
    }

    @Test
    fun testDecimal32AddConstFloat() {
        generate("decimal32_add_const_float", """
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
                    from $tableName 
                    limit 10
                    """)
    }


    @Test
    fun testDecimalLiteralOperation() {
        generate("decimal_literal_operation", """
                    select 
                          3.14 + 2.667 as add_result0
                        , cast(3.14 as decimal32(9,2)) + 2.667 as add_result1
                        , 3.14 + cast(2.667 as decimal32(9,3)) as add_result2
                        , cast('3.14' as decimal32(9,2)) + cast('2.667' as decimal32(9,3)) as add_result3
                        , cast('3.14' as decimal) + cast('2.667' as decimal) as add_result4
                        , cast(3.14 as decimal64(9,2)) + cast('2.667' as decimal64(9,3)) as add_result5
                        , cast(3.14 as decimal128(9,2)) + cast('2.667' as decimal128(9,3)) as add_result6
                        , cast(3.14 as decimal32(9,2)) + cast('2.667' as decimal64(18,3)) as add_result7
                        , cast(3.14 as decimal32(9,2)) + cast('2.667' as decimal128(18,3)) as add_result8
                        , cast(3.14 as decimal64(9,2)) + cast('2.667' as decimal128(18,3)) as add_result9
                        
                        , 3.14 - 2.667 as sub_result0
                        , cast(3.14 as decimal32(9,2)) - 2.667 as sub_result1
                        , 3.14 - cast(2.667 as decimal32(9,3)) as sub_result2
                        , cast('3.14' as decimal32(9,2)) - cast('2.667' as decimal32(9,3)) as sub_result3
                        , cast('3.14' as decimal) - cast('2.667' as decimal) as sub_result4
                        , cast(3.14 as decimal64(9,2)) - cast('2.667' as decimal64(9,3)) as sub_result5
                        , cast(3.14 as decimal128(9,2)) - cast('2.667' as decimal128(9,3)) as sub_result6
                        , cast(3.14 as decimal32(9,2)) - cast('2.667' as decimal64(18,3)) as sub_result7
                        , cast(3.14 as decimal32(9,2)) - cast('2.667' as decimal128(18,3)) as sub_result8
                        , cast(3.14 as decimal64(9,2)) - cast('2.667' as decimal128(18,3)) as sub_result9
                        
                        , 3.14 * 2.667 as mul_result10
                        , cast(3.14 as decimal32(9,2)) * 2.667 as mul_result11
                        , 3.14 * cast(2.667 as decimal32(9,3)) as mul_result12
                        , cast('3.14' as decimal32(9,2)) * cast('2.667' as decimal32(9,3)) as mul_result13
                        , cast('3.14' as decimal) * cast('2.667' as decimal) as sub_result14
                        , cast(3.14 as decimal64(9,2)) * cast('2.667' as decimal64(9,3)) as mul_result15
                        , cast(3.14 as decimal128(9,2)) * cast('2.667' as decimal128(9,3)) as mul_result16
                        , cast(3.14 as decimal32(9,2)) * cast('2.667' as decimal64(18,3)) as mul_result17
                        , cast(3.14 as decimal32(9,2)) * cast('2.667' as decimal128(18,3)) as mul_result18
                        , cast(3.14 as decimal64(9,2)) * cast('2.667' as decimal128(18,3)) as mul_result19
                        
                        , 3.14 / 2.667 as div_result0
                        , cast(3.14 as decimal32(9,2)) / 2.667 as div_result1
                        , 3.14 / cast(2.667 as decimal32(9,3)) as div_result2
                        , cast('3.14' as decimal32(9,2)) / cast('2.667' as decimal32(9,3)) as div_result3
                        , cast('3.14' as decimal) / cast('2.667' as decimal) as div_result4
                        , cast(3.14 as decimal64(9,2)) / cast('2.667' as decimal64(9,3)) as div_result5
                        , cast(3.14 as decimal128(9,2)) / cast('2.667' as decimal128(9,3)) as div_result6
                        , cast(3.14 as decimal32(9,2)) / cast('2.667' as decimal64(18,3)) as div_result7
                        , cast(3.14 as decimal32(9,2)) / cast('2.667' as decimal128(18,3)) as div_result8
                        , cast(3.14 as decimal64(9,2)) / cast('2.667' as decimal128(18,3)) as div_result9
                        
                        , 3.14 % 2.667 as mod_result0
                        , cast(3.14 as decimal32(9,2)) % 2.667 as mod_result1
                        , 3.14 % cast(2.667 as decimal32(9,3)) as mod_result2
                        , cast('3.14' as decimal32(9,2)) % cast('2.667' as decimal32(9,3)) as mod_result3
                        , cast('3.14' as decimal) % cast('2.667' as decimal) as mod_result4
                        , cast(3.14 as decimal64(9,2)) % cast('2.667' as decimal64(9,3)) as mod_result5
                        , cast(3.14 as decimal128(9,2)) % cast('2.667' as decimal128(9,3)) as mod_result6
                        , cast(3.14 as decimal32(9,2)) % cast('2.667' as decimal64(18,3)) as mod_result7
                        , cast(3.14 as decimal32(9,2)) % cast('2.667' as decimal128(18,3)) as mod_result8
                        , cast(3.14 as decimal64(9,2)) % cast('2.667' as decimal128(18,3)) as mod_result9
                        from $tableName
                        limit 10
                    """)
    }

    @Test
    fun testDecimalLiteralAddOtherLiteral() {
        generate("decimal_literal_and_other_literal", """
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
        generate("decimal_literal_sub_other_literal", """
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
        generate("decimal_literal_mul_other_literal", """
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
        generate("decimal_literal_div_other_literal", """
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
        generate("decimalv2_literal_and_other_literal", """
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
        generate("decimalv2_literal_sub_other_literal", """
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
        generate("decimalv2_literal_mul_other_literal", """
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
        generate("decimalv2_literal_div_other_literal", """
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
        generate("decimalv2_literal_mod_other_literal", """
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
        generate("decimal_literal_mod_other_literal", """
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
    fun testDecimalV2AndLiteralOp() {
        generate("decimalv2_and_literal_op", """
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
                    from $tableName
                    limit 10
                    """)
    }

    @Test
    fun testDecimalV2AndDecimal32Op() {
        generate("decimalv2_and_decimal32_op", """
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
                    from $tableName
                    limit 40
                    """)
    }

    @Test
    fun testDecimalV2AndDecimal64Op() {
        generate("decimalv2_and_decimal64_op", """
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
                    from $tableName
                    limit 40
                    """)
    }

    @Test
    fun testDecimalV2AndDecimal128Op() {
        generate("decimalv2_and_decimal128_op", """
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
                    from $tableName
                    limit 40
                    """)
    }

    @Test
    fun testDecimalMoneyFormat() {
        generate("decimal_money_format",
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
                    from $tableName 
                    limit 40
                """.trimIndent()
        )
    }

    @Test
    fun testDecimalAbs() {
        generate("decimal_abs",
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
                    from $tableName 
                    limit 40
                """.trimIndent()
        )
    }

    @Test
    fun testDecimalNegative() {
        generate("decimal_negative",
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
                    from $tableName 
                    limit 40
                """.trimIndent()
        )
    }

    @Test
    fun testDecimalPositive() {
        generate("positive",
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
                    from $tableName 
                    limit 40
                """.trimIndent()
        )
    }

    @Test
    fun testDecimalModConst() {
        generate("decimal_mod_const",
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
                    from $tableName 
                    limit 40
                """.trimIndent()
        )
    }

    @Test
    fun testDecimalLeastAndGreatest() {
        generate("decimal_least_and_greatest",
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
                    from $tableName 
                    limit 40
                """.trimIndent()
        )
    }

    @Test
    fun testDecimalDecimalIfConst() {
        generate("decimal_if_const",
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
                      from $tableName
                      limit 10
                      
              """.trimIndent())
    }

    @Test
    fun testDecimalDecimalIf() {
        generate("decimal_if",
                """
                    select
                      1
                      , col0_decimal_p9s2, if(col0_decimal_p9s2 >= 0, col0_decimal_p9s2, - col0_decimal_p9s2) as res0
                      , col_decimal_p18s18, if(col_decimal_p18s18 >= 0, col_decimal_p18s18, - col_decimal_p18s18) as res1
                      , col_decimal_p38s38, if(col_decimal_p38s38 >= 0, col_decimal_p38s38, - col_decimal_p38s38) as res2
                      , col_nullable_decimal_p9s9, col_nullable_decimal_p9s9 is NULL as is_null0, if(col_nullable_decimal_p9s9 is NULL, 0, col_nullable_decimal_p9s9) as res3
                      , col_nullable_decimal_p15s10, col_nullable_decimal_p15s10 is NULL as is_null1, if(col_nullable_decimal_p15s10 is NULL, 0, col_nullable_decimal_p15s10) as res4
                      , col_nullable_decimal_p33s17, col_nullable_decimal_p33s17 is NULL as is_null2, if(col_nullable_decimal_p33s17 is NULL, 0, col_nullable_decimal_p33s17) as res5
                    from $tableName
                    limit 10
              """.trimIndent())
    }

    @Test
    fun testDecimalDecimalIfNull() {
        generate("decimal_ifnull",
                """
                    select
                      1
                      , col0_decimal_p9s2, ifnull(0, col0_decimal_p9s2) as res0
                      , col_decimal_p18s18, ifnull(null, col_decimal_p18s18) as res1
                      , col_decimal_p38s38, ifnull(1, col_decimal_p38s38) as res2
                      , col_nullable_decimal_p9s9,  ifnull(col_nullable_decimal_p9s9, 3.14) as res3
                      , col_nullable_decimal_p15s10, ifnull(col_nullable_decimal_p15s10, 3.14) as res4
                      , col_nullable_decimal_p33s17,  ifnull(col_nullable_decimal_p33s17, 3.14) as res5
                    from $tableName
                    limit 10
              """.trimIndent())
    }

    @Test
    fun testDecimalDecimalNullIf() {
        generate("decimal_nullif",
                """
                    select
                      1
                      , col0_decimal_p9s2, nullif(cast('-589962.71' as decimal32(9,3)), col0_decimal_p9s2) as res0
                      , col_decimal_p18s18, nullif(col_decimal_p18s18, cast(1E-18 as decimal64(18,18))) as res1
                      , col_decimal_p38s38, nullif(cast('-0.99999999999999999999999039773745784102' as decimal128(38,38)), col_decimal_p38s38) as res2
                      , col_nullable_decimal_p9s9,  nullif(col_nullable_decimal_p9s9, cast('-47474.9023850552' as decimal32(9,9))) as res3
                      , col_nullable_decimal_p15s10,  nullif(col_nullable_decimal_p15s10, null) as res4
                      , col_nullable_decimal_p33s17,  nullif(null, col_nullable_decimal_p33s17) as res5
                    from $tableName
                    limit 10
              """.trimIndent())
    }

    @Test
    fun testDecimalDecimalCoalesce() {
        generate("decimal_coalesce",
                """
                    select
                      1
                      , coalesce(cast('-589962.71' as decimal32(9,3)), col0_decimal_p9s2) as res0
                      , coalesce(col_decimal_p18s18, cast(1E-18 as decimal64(18,18))) as res1
                      , coalesce(cast('-0.99999999999999999999999039773745784102' as decimal128(38,38)), col_decimal_p38s38) as res2
                      , col_nullable_decimal_p9s9, coalesce(col_nullable_decimal_p9s9, null, cast('-47474.9023850552' as decimal32(9,9))) as res3
                      , col_nullable_decimal_p15s10, coalesce(col_nullable_decimal_p15s10, null, null, 3.14) as res4
                      , col_nullable_decimal_p33s17, coalesce(null, col_nullable_decimal_p33s17, null,null,null,null) as res5
                    from $tableName
                    limit 10
              """.trimIndent())
    }

    @Test
    fun testDecimal32p9s2Aggregation() {
        generate("decimal32p9s2_aggregation",
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
                    from $tableName 
                """.trimIndent()
        )
    }

    @Test
    fun testNullableDecimal32p9s6Aggregation() {
        generate("decimal32p9s6_aggregation",
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
                    from $tableName 
                """.trimIndent()
        )
    }

    @Test
    fun testDecimal64p15s3Aggregation() {
        generate("decimal64p15s3_aggregation",
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
                    from $tableName 
                """.trimIndent()
        )
    }

    @Test
    fun testNullableDecimal64p15s15Aggregation() {
        generate("decimal64p15s15_aggregation",
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
                    from $tableName 
                """.trimIndent()
        )
    }

    @Test
    fun testDecimal128p38s33Aggregation() {
        generate("decimal128p38s33_aggregation",
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
                    from $tableName 
                """.trimIndent()
        )
    }

    @Test
    fun testNullableDecimal128p33s17Aggregation() {
        generate("nullable_decimal128p33s17_aggregation",
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
                    from $tableName 
                """.trimIndent()
        )
    }
}

