package com.grakra.schema

import org.testng.annotations.Test

class TestSchema {
    @Test
    fun test() {
        val fields = listOf(
                SimpleField.fixedLength("boolean_col", FixedLengthType.TYPE_BOOLEAN),
                SimpleField.fixedLength("tinyint_col", FixedLengthType.TYPE_TINYINT),
                SimpleField.fixedLength("smallint_col", FixedLengthType.TYPE_SMALLINT),
                SimpleField.fixedLength("int_col", FixedLengthType.TYPE_INT),
                SimpleField.fixedLength("bigint_col", FixedLengthType.TYPE_BIGINT),
                SimpleField.fixedLength("largeint_col", FixedLengthType.TYPE_LARGEINT),
                SimpleField.fixedLength("date_col", FixedLengthType.TYPE_DATE),
                SimpleField.fixedLength("timestamp_col", FixedLengthType.TYPE_DATETIME),
                SimpleField.fixedLength("float_col", FixedLengthType.TYPE_FLOAT),
                SimpleField.fixedLength("double_col", FixedLengthType.TYPE_DOUBLE),
                SimpleField.fixedLength("decimalv2_col", FixedLengthType.TYPE_DECIMALV2),
                SimpleField.char("char_col", 255),
                SimpleField.varchar("varchar_col", 65536),
                SimpleField.decimal("decimal_p9s2_col0", 32, 9, 2),
                SimpleField.decimal("decimal_p9s2_col1", 32, 9, 2),
                SimpleField.decimal("decimal_p15s6_col0", 64, 15, 6),
                SimpleField.decimal("decimal_p15s6_col1", 64, 15, 6),
                SimpleField.decimal("decimal_p15s3_col0", 64, 15, 3),
                SimpleField.decimal("decimal_p15s3_col1", 64, 15, 3),
                SimpleField.decimal("decimal_p38s6_col0", 128, 38, 6),
                SimpleField.decimal("decimal_p38s6_col1", 128, 38, 6),
                SimpleField.decimal("decimal_p38s3_col0", 128, 38, 3),
                SimpleField.decimal("decimal_p38s3_col1", 128, 38, 3),

                CompoundField.nullable(SimpleField.fixedLength("nullable_boolean_col", FixedLengthType.TYPE_BOOLEAN), 50),
                CompoundField.nullable(SimpleField.fixedLength("nullable_tinyint_col", FixedLengthType.TYPE_TINYINT), 50),
                CompoundField.nullable(SimpleField.fixedLength("nullable_smallint_col", FixedLengthType.TYPE_SMALLINT), 50),
                CompoundField.nullable(SimpleField.fixedLength("nullable_int_col", FixedLengthType.TYPE_INT), 50),
                CompoundField.nullable(SimpleField.fixedLength("nullable_bigint_col", FixedLengthType.TYPE_BIGINT), 50),
                CompoundField.nullable(SimpleField.fixedLength("nullable_largeint_col", FixedLengthType.TYPE_LARGEINT), 50),
                CompoundField.nullable(SimpleField.fixedLength("nullable_date_col", FixedLengthType.TYPE_DATE), 50),
                CompoundField.nullable(SimpleField.fixedLength("nullable_timestamp_col", FixedLengthType.TYPE_DATETIME), 50),
                CompoundField.nullable(SimpleField.fixedLength("nullable_float_col", FixedLengthType.TYPE_FLOAT), 50),
                CompoundField.nullable(SimpleField.fixedLength("nullable_double_col", FixedLengthType.TYPE_DOUBLE), 50),
                CompoundField.nullable(SimpleField.fixedLength("nullable_decimalv2_col", FixedLengthType.TYPE_DECIMALV2), 50),
                CompoundField.nullable(SimpleField.char("nullable_char_col", 255), 50),
                CompoundField.nullable(SimpleField.varchar("nullable_varchar_col", 65536), 50),
                CompoundField.nullable(SimpleField.decimal("nullable_decimal_p9s2_col0", 32, 9, 2), 50),
                CompoundField.nullable(SimpleField.decimal("nullable_decimal_p9s2_col1", 32, 9, 2), 50),
                CompoundField.nullable(SimpleField.decimal("nullable_decimal_p15s6_col0", 64, 15, 6), 50),
                CompoundField.nullable(SimpleField.decimal("nullable_decimal_p15s6_col1", 64, 15, 6), 50),
                CompoundField.nullable(SimpleField.decimal("nullable_decimal_p15s3_col0", 64, 15, 3), 50),
                CompoundField.nullable(SimpleField.decimal("nullable_decimal_p15s3_col1", 64, 15, 3), 50),
                CompoundField.nullable(SimpleField.decimal("nullable_decimal_p38s6_col0", 128, 38, 6), 50),
                CompoundField.nullable(SimpleField.decimal("nullable_decimal_p38s6_col1", 128, 38, 6), 50),
                CompoundField.nullable(SimpleField.decimal("nullable_decimal_p38s3_col0", 128, 38, 3), 50),
                CompoundField.nullable(SimpleField.decimal("nullable_decimal_p38s3_col1", 128, 38, 3), 50))

        OrcUtil.createOrcFile("file0.orc", fields, 4097, 4096)
        OrcUtil.readOrcFile("file0.orc")
    }
}