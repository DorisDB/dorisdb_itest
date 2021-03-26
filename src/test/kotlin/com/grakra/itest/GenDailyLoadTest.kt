package com.grakra.itest

import com.grakra.TestMethodCapture
import com.grakra.dorisdb.HiveClient
import com.grakra.schema.*
import com.grakra.util.Util
import org.testng.annotations.Listeners
import org.testng.annotations.Test

@Listeners(TestMethodCapture::class)
class GenDailyLoadTest : DorisDBRemoteITest() {
    val fields = listOf(
            SimpleField.fixedLength("col_date", FixedLengthType.TYPE_DATE),
            SimpleField.fixedLength("col_datetime", FixedLengthType.TYPE_DATETIME),
            SimpleField.char("col_char", 20),
            SimpleField.varchar("col_varchar", 20),
            SimpleField.fixedLength("col_boolean", FixedLengthType.TYPE_BOOLEAN),
            SimpleField.fixedLength("col_tinyint", FixedLengthType.TYPE_TINYINT),
            SimpleField.fixedLength("col_smallint", FixedLengthType.TYPE_SMALLINT),
            SimpleField.fixedLength("col_int", FixedLengthType.TYPE_INT),
            SimpleField.fixedLength("col_bigint", FixedLengthType.TYPE_BIGINT),
            SimpleField.fixedLength("col_float", FixedLengthType.TYPE_FLOAT),
            SimpleField.fixedLength("col_double", FixedLengthType.TYPE_DOUBLE),
            SimpleField.decimalv2("col_decimal_p6s2", 6, 2),
            SimpleField.decimalv2("col_decimal_p14s5", 14, 5),
            SimpleField.decimalv2("col_decimal_p27s9", 27, 9),
            SimpleField.decimal("col_decimal32_p6s2", 32, 6, 2),
            SimpleField.decimal("col_decimal64_p14s5", 64, 14, 5),
            SimpleField.decimal("col_decimal128_p27s9", 128, 33, 9)
    )
    val nullableFields = fields.map { CompoundField.nullable(it, 50) }

    val table = Table("table_with_decimal_v3", fields, 3)
    val nullableTable = Table("nullable_table_with_decimal_v3", nullableFields, 3)
    val orcTable = table.renameTable("orc_table_with_decimal_v3")
    val parquetTable = table.renameTable("parquet_table_with_decimal_v3")
    val orcNullableTable = nullableTable.renameTable("orc_nullable_table_with_decimal_v3")
    val parquetNullableTable = nullableTable.renameTable("parquet_nullable_table_with_decimal_v3")

    @Test
    fun generate_orc_files() {
        val numRowsArray = arrayOf(0, 1, 4095, 4096, 4097, 64 * 1024 - 1, 64 * 1024, 64 * 1024 + 1)
        val nullablePrefix = "decimal_v3_load_test/orc/nullable"
        val prefix = "decimal_v3_load_test/orc/not_nullable"
        Util.recreateDir(nullablePrefix)
        Util.recreateDir(prefix)
        numRowsArray.forEach { numRows ->
            table.createOrcFile(prefix, numRows)
            nullableTable.createOrcFile(nullablePrefix, numRows)
        }
    }

    @Test
    fun generate_parquet_files() {
        val orcFiles = arrayOf(
                "/decimal_v3_load_test/orc/not_nullable/table_with_decimal_v3_0.orc",
                "/decimal_v3_load_test/orc/not_nullable/table_with_decimal_v3_1.orc",
                "/decimal_v3_load_test/orc/not_nullable/table_with_decimal_v3_4095.orc",
                "/decimal_v3_load_test/orc/not_nullable/table_with_decimal_v3_4096.orc",
                "/decimal_v3_load_test/orc/not_nullable/table_with_decimal_v3_4097.orc",
                "/decimal_v3_load_test/orc/not_nullable/table_with_decimal_v3_65535.orc",
                "/decimal_v3_load_test/orc/not_nullable/table_with_decimal_v3_65536.orc",
                "/decimal_v3_load_test/orc/not_nullable/table_with_decimal_v3_65537.orc"
        )
        val nullableOrcFiles = arrayOf(
                "/decimal_v3_load_test/orc/nullable/nullable_table_with_decimal_v3_0.orc",
                "/decimal_v3_load_test/orc/nullable/nullable_table_with_decimal_v3_1.orc",
                "/decimal_v3_load_test/orc/nullable/nullable_table_with_decimal_v3_4095.orc",
                "/decimal_v3_load_test/orc/nullable/nullable_table_with_decimal_v3_4096.orc",
                "/decimal_v3_load_test/orc/nullable/nullable_table_with_decimal_v3_4097.orc",
                "/decimal_v3_load_test/orc/nullable/nullable_table_with_decimal_v3_65535.orc",
                "/decimal_v3_load_test/orc/nullable/nullable_table_with_decimal_v3_65536.orc",
                "/decimal_v3_load_test/orc/nullable/nullable_table_with_decimal_v3_65537.orc"
        )
        val hiveClient = HiveClient("127.0.0.1:10000/default", "grakra", "")
        table.createHiveParquetScripts(hiveClient, orcFiles, "orc")
        nullableTable.createHiveParquetScripts(hiveClient, nullableOrcFiles, "orc")
    }

    val db = "decimal_v3_load_test"
    @Test
    fun broker_load_orc() {
        val prefix = "/user"
        create_db(db)
        admin_set_vectorized_load_enable(true)
        broker_load_and_compute_fingerprint(db, orcTable, "orc", "$prefix/decimal_v3_load_test/orc/not_nullable/*.orc")
        broker_load_and_compute_fingerprint(db, orcNullableTable, "orc", "$prefix/decimal_v3_load_test/orc/nullable/*.orc")
        broker_load_and_compute_fingerprint(db, parquetTable, "parquet", "$prefix/decimal_v3_load_test/parquet/not_nullable/*.parquet")
        broker_load_and_compute_fingerprint(db, parquetNullableTable, "parquet", "$prefix/decimal_v3_load_test/parquet/nullable/*.parquet")
    }

    @Test
    fun compare_parquet_and_orc_load() {
        compare_each_column(db, orcTable.tableName, parquetTable.tableName, orcTable.fields.map { it.name })
        compare_each_column(db, orcNullableTable.tableName, parquetNullableTable.tableName, orcNullableTable.fields.map { it.name })
    }
}