package com.grakra.itest

import com.grakra.TestMethodCapture
import com.grakra.schema.FixedLengthType
import com.grakra.schema.OrcUtil
import com.grakra.schema.SimpleField
import com.grakra.schema.Table
import com.grakra.util.Util
import org.testng.annotations.Listeners
import org.testng.annotations.Test

@Listeners(TestMethodCapture::class)
class InsertTest : DorisDBRemoteITest() {
    val db = "decimal_insert_test_db"
    val table1Name = "table1"
    val table2Name = "table2"
    val table1Fields = listOf(
            SimpleField.fixedLength("col_int", FixedLengthType.TYPE_INT),
            SimpleField.decimalv2("col_decimalv2_p18_s4", 18, 4),
            SimpleField.decimal("col_decimal32_p7_s2", 32, 7, 2),
            SimpleField.decimal("col_decimal64_p13_s5", 64, 13, 5),
            SimpleField.decimal("col_decimal128_p24_s11", 128, 24, 11)
    )

    val table2Fields = listOf(
            SimpleField.fixedLength("col_int", FixedLengthType.TYPE_INT),
            SimpleField.decimalv2("col_decimalv2_p18_s4", 18, 4),
            SimpleField.decimal("col_decimal32_p7_s2", 32, 7, 2),
            SimpleField.decimal("col_decimal64_p13_s5", 64, 13, 5),
            SimpleField.decimal("col_decimal128_p24_s11", 128, 24, 11)
    )

    val table1 = Table(table1Name, table1Fields, 1)
    val table2 = Table(table2Name, table2Fields, 1)

    @Test
    fun test_insert_into_values(){
        create_db(db)
        create_table(db, table1)
        val orcPath = "$db.${table1.tableName}.orc"
        OrcUtil.createOrcFile(orcPath, table1.keyFields(), table1.valueFields(setOf()), 10, 4096)
        val fieldNames = table1.fields.map{it.name}.toTypedArray()
        val tuples = OrcUtil.orcToList(orcPath, *fieldNames)
        val sql = table1.insertIntoValuesSql(tuples)
        execute(db, sql)
        query_print(db, "select * from ${table1.tableName}")
    }

    @Test
    fun test_insert_into_select(){
        create_db(db)
        create_table(db, table1)
        val orcPath = "$db.${table1.tableName}.orc"
        OrcUtil.createOrcFile(orcPath, table1.keyFields(), table1.valueFields(setOf()), 10, 4096)
        val fieldNames = table1.fields.map{it.name}.toTypedArray()
        val tuples = OrcUtil.orcToList(orcPath, *fieldNames)
        val sql1 = table1.insertIntoValuesSql(tuples)
        execute(db, sql1)
        val selectSql1 = "select * from ${table1.tableName}"
        query_print(db, selectSql1)
        create_table(db, table2)
        val sql2 = table2.insertIntoSubQuerySql(selectSql1)
        execute(db, sql2)
        val selectSql2 = "select * from ${table2.tableName}"
        query_print(db, selectSql2)
        compare_two_tables(db, db,table1.tableName, table2.tableName)
    }

    @Test
    fun test_orc_to_list(){
        val orcPath = Util.getTestName(this)
        OrcUtil.createOrcFile(orcPath, table1.keyFields(), table1.valueFields(setOf()), 10, 4096)
        val fieldNames = table1.fields.map{it.name}.toTypedArray()
        val tuples = OrcUtil.orcToList(orcPath, *fieldNames)
        for (t in tuples){
            println(t.joinToString(","))
        }
    }
}