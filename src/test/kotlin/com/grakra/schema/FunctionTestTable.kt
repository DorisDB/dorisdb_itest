package com.grakra.schema

import com.grakra.util.Util

object FunctionTestTable {
    val keyFields = List(3) { SimpleField.fixedLength("key$it", FixedLengthType.TYPE_INT) }
    val valueFields = listOf(
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
            SimpleField.fixedLength("col_decimalv2", FixedLengthType.TYPE_DECIMALV2),
            SimpleField.char("col_char", 255),
            SimpleField.varchar("col_varchar", 65533),
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
            CompoundField.nullable(SimpleField.fixedLength("col_nullable_decimalv2", FixedLengthType.TYPE_DECIMALV2), 50),
            CompoundField.nullable(SimpleField.char("col_nullable_char", 255), 50),
            CompoundField.nullable(SimpleField.varchar("col_nullable_varchar", 65533), 50),
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


    fun brokerLoadSql(db: String, table: String): String {
        return Util.renderTemplate("broker_load.sql.template",
                "db" to db,
                "table" to table,
                "labelId" to System.currentTimeMillis().toString(),
                "hdfsPath" to "/rpf/orc_files/decimal_all_types.orc",
                "format" to "orc",
                "columnList" to (keyFields + valueFields).map { it.name })

    }

    fun createTableSql(table: String): String {
        return Table(table,
                FunctionTestTable.keyFields + FunctionTestTable.valueFields,
                FunctionTestTable.keyFields.size).sql()
    }

    fun createOrcFile() {
        OrcUtil.createOrcFile(
                "decimal_all_types.orc",
                FunctionTestTable.keyFields,
                FunctionTestTable.valueFields,
                40971,
                4096)
    }

    fun readOrcFile(vararg fields: String) {
        OrcUtil.readOrcFile("decimal_all_types.orc", *fields)
    }
}