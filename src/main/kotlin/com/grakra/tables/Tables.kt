package com.grakra.tables

import com.grakra.schema.FixedLengthType
import com.grakra.schema.SimpleField
import com.grakra.schema.Table

object Tables {
    val decimal_perf_table = Table("decimal_perf_table", listOf(
            SimpleField.fixedLength("id", FixedLengthType.TYPE_BIGINT),
            SimpleField.decimal("col0_i32p7s2", 32, 7, 2),
            SimpleField.decimal("col1_i32p7s2", 32, 7, 2),
            SimpleField.decimal("col0_i32p6s3", 32, 6, 3),
            SimpleField.decimal("col1_i32p6s3", 32, 6, 3),
            SimpleField.decimal("col0_i64p7s2", 64, 7, 2),
            SimpleField.decimal("col1_i64p7s2", 64, 7, 2),
            SimpleField.decimal("col0_i64p9s5", 64, 9, 5),
            SimpleField.decimal("col1_i64p9s5", 64, 9, 5),
            SimpleField.decimal("col0_i128p7s2", 128, 7, 2),
            SimpleField.decimal("col1_i128p7s2", 128, 7, 2),
            SimpleField.decimal("col0_i128p18s9", 128, 18, 9),
            SimpleField.decimal("col1_i128p18s9", 128, 18, 9),
            SimpleField.decimal("col0_i128p30s9", 128, 30, 9),
            SimpleField.decimal("col1_i128p30s9", 128, 30, 9),
            SimpleField.fixedLength("col_float", FixedLengthType.TYPE_FLOAT),
            SimpleField.fixedLength("col_double", FixedLengthType.TYPE_DOUBLE),
            SimpleField.varchar("col_varchar", 50),
            SimpleField.char("col_char", 50),
            SimpleField.fixedLength("col_tinyint", FixedLengthType.TYPE_TINYINT),
            SimpleField.fixedLength("col_smallint", FixedLengthType.TYPE_SMALLINT),
            SimpleField.fixedLength("col_int", FixedLengthType.TYPE_INT),
            SimpleField.fixedLength("col_bigint", FixedLengthType.TYPE_BIGINT),
            SimpleField.fixedLength("col_largeint", FixedLengthType.TYPE_LARGEINT)),
            1
    )

    val decimal128_table = Table("decimal128_table", listOf(
            SimpleField.fixedLength("id", FixedLengthType.TYPE_BIGINT),
            SimpleField.decimal("col0_i128p7s2", 128, 7, 2),
            SimpleField.decimal("col1_i128p7s2", 128, 7, 2),
            SimpleField.decimal("col0_i128p18s9", 128, 18, 9),
            SimpleField.decimal("col1_i128p18s9", 128, 18, 9),
            SimpleField.fixedLength("col_double", FixedLengthType.TYPE_DOUBLE),
            SimpleField.char("col_char", 50),
            SimpleField.fixedLength("col_int", FixedLengthType.TYPE_INT)),
            1
    )

    val decimalv2_table = Table("decimalv2_table", listOf(
            SimpleField.fixedLength("id", FixedLengthType.TYPE_BIGINT),
            SimpleField.decimalv2("col0_i128p7s2", 7, 2),
            SimpleField.decimalv2("col1_i128p7s2", 7, 2),
            SimpleField.decimalv2("col0_i128p18s9", 18, 9),
            SimpleField.decimalv2("col1_i128p18s9", 18, 9),
            SimpleField.fixedLength("col_double", FixedLengthType.TYPE_DOUBLE),
            SimpleField.char("col_char", 50),
            SimpleField.fixedLength("col_int", FixedLengthType.TYPE_INT)),
            1)

    val decimal128_key_table = Table("decimal128_key_table", listOf(
            SimpleField.decimal("col0_i128p7s2", 128, 7, 2),
            SimpleField.fixedLength("id", FixedLengthType.TYPE_BIGINT),
            SimpleField.decimal("col1_i128p7s2", 128, 7, 2),
            SimpleField.decimal("col0_i128p18s9", 128, 18, 9),
            SimpleField.decimal("col1_i128p18s9", 128, 18, 9),
            SimpleField.fixedLength("col_double", FixedLengthType.TYPE_DOUBLE),
            SimpleField.char("col_char", 50),
            SimpleField.fixedLength("col_int", FixedLengthType.TYPE_INT)),
            1
    )

    val decimalv2_key_table = Table("decimalv2_key_table", listOf(
            SimpleField.decimalv2("col0_i128p7s2", 7, 2),
            SimpleField.fixedLength("id", FixedLengthType.TYPE_BIGINT),
            SimpleField.decimalv2("col1_i128p7s2", 7, 2),
            SimpleField.decimalv2("col0_i128p18s9", 18, 9),
            SimpleField.decimalv2("col1_i128p18s9", 18, 9),
            SimpleField.fixedLength("col_double", FixedLengthType.TYPE_DOUBLE),
            SimpleField.char("col_char", 50),
            SimpleField.fixedLength("col_int", FixedLengthType.TYPE_INT)),
            1)

    val decimal128_value_table = Table("decimal128_value_table", listOf(
            SimpleField.fixedLength("id", FixedLengthType.TYPE_BIGINT),
            SimpleField.decimal("col0_i128p7s2", 128, 7, 2),
            SimpleField.decimal("col1_i128p7s2", 128, 7, 2),
            SimpleField.decimal("col0_i128p18s9", 128, 18, 9),
            SimpleField.decimal("col1_i128p18s9", 128, 18, 9),
            SimpleField.fixedLength("col_double", FixedLengthType.TYPE_DOUBLE),
            SimpleField.char("col_char", 50),
            SimpleField.fixedLength("col_int", FixedLengthType.TYPE_INT)),
            1
    )

    val decimalv2_value_table = Table("decimalv2_value_table", listOf(
            SimpleField.fixedLength("id", FixedLengthType.TYPE_BIGINT),
            SimpleField.decimalv2("col0_i128p7s2", 7, 2),
            SimpleField.decimalv2("col1_i128p7s2", 7, 2),
            SimpleField.decimalv2("col0_i128p18s9", 18, 9),
            SimpleField.decimalv2("col1_i128p18s9", 18, 9),
            SimpleField.fixedLength("col_double", FixedLengthType.TYPE_DOUBLE),
            SimpleField.char("col_char", 50),
            SimpleField.fixedLength("col_int", FixedLengthType.TYPE_INT)),
            1)
}