package com.grakra.tools

import com.grakra.schema.FixedLengthType
import com.grakra.schema.OrcUtil
import com.grakra.schema.SimpleField
import com.grakra.schema.Table
import kotlin.system.exitProcess

fun main(vararg args: String) {
    if (args.size < 3) {
        println("missing <numRows>")
        exitProcess(1)
    }
    val filePrefix = args[0]
    val numFiles = args[1].toInt()
    val numRowsPerFile = args[2].toInt()
    val table = Table("decimal_test_table", listOf(
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
            SimpleField.decimal("col1_i128p7s2", 129, 7, 2),
            SimpleField.decimal("col0_i128p18s9", 128, 18, 9),
            SimpleField.decimal("col1_i128p18s9", 129, 18, 9),
            SimpleField.decimal("col0_i128p30s9", 128, 30, 9),
            SimpleField.decimal("col1_i128p30s9", 129, 30, 9),
            SimpleField.fixedLength("col_float", FixedLengthType.TYPE_FLOAT),
            SimpleField.fixedLength("col_float", FixedLengthType.TYPE_DOUBLE),
            SimpleField.varchar("col_varchar", 50),
            SimpleField.char("col_varchar", 50),
            SimpleField.fixedLength("col_tinyint", FixedLengthType.TYPE_TINYINT),
            SimpleField.fixedLength("col_smallint", FixedLengthType.TYPE_SMALLINT),
            SimpleField.fixedLength("col_int", FixedLengthType.TYPE_INT),
            SimpleField.fixedLength("col_bigint", FixedLengthType.TYPE_BIGINT),
            SimpleField.fixedLength("col_largeint", FixedLengthType.TYPE_LARGEINT)),
            1
    )

    (1..numFiles).forEach { n->
        val suffix = "0000$n".takeLast(4)
        val fileName = "${filePrefix}_${suffix}.orc"
        OrcUtil.createOrcFile(fileName, table.keyFields(), table.valueFields(emptySet()), numRowsPerFile,65536);
    }
    //OrcUtil.createOrcFile()
}