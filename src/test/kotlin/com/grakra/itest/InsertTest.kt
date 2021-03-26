package com.grakra.itest

import com.grakra.TestMethodCapture
import com.grakra.schema.*
import com.grakra.util.Util
import org.testng.Assert
import org.testng.annotations.Listeners
import org.testng.annotations.Test
import java.io.File

@Listeners(TestMethodCapture::class)
class InsertTest : DorisDBRemoteITest() {
    val db = "decimal_insert_test_db"
    val fields = listOf(
            SimpleField.fixedLength("col_int", FixedLengthType.TYPE_INT),
            SimpleField.fixedLength("col_bigint", FixedLengthType.TYPE_BIGINT),
            SimpleField.fixedLength("col_double", FixedLengthType.TYPE_DOUBLE),
            SimpleField.decimalv2("col_decimalv2_p18_s4", 18, 4),
            SimpleField.decimal("col_decimal32_p7_s2", 32, 7, 2),
            SimpleField.decimal("col_decimal64_p13_s5", 64, 13, 5),
            SimpleField.decimal("col_decimal128_p24_s11", 128, 24, 11)
    )

    val table1 = Table("table1", fields, 1)
    val table2 = table1.renameTable("table2")

    @Test
    fun test_insert_into_values() {
        create_db(db)
        create_table(db, table1)
        val orcPath = "$db.${table1.tableName}.orc"
        OrcUtil.createOrcFile(orcPath, table1.keyFields(), table1.valueFields(setOf()), 10, 4096)
        val fieldNames = table1.fields.map { it.name }.toTypedArray()
        val tuples = OrcUtil.orcToList(orcPath, *fieldNames)
        val sql = table1.insertIntoValuesSql(tuples)
        execute(db, sql)
        query_print(db, "select * from ${table1.tableName}")
    }

    @Test
    fun test_insert_into_select() {
        create_db(db)
        create_table(db, table1)
        val orcPath = "$db.${table1.tableName}.orc"
        OrcUtil.createOrcFile(orcPath, table1.keyFields(), table1.valueFields(setOf()), 10, 4096)
        val fieldNames = table1.fields.map { it.name }.toTypedArray()
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
        compare_two_tables(db, db, table1.tableName, table2.tableName)
    }

    @Test
    fun test_orc_to_list() {
        val orcPath = Util.getTestName(this)
        OrcUtil.createOrcFile(orcPath, table1.keyFields(), table1.valueFields(setOf()), 10, 4096)
        val fieldNames = table1.fields.map { it.name }.toTypedArray()
        val tuples = OrcUtil.orcToList(orcPath, *fieldNames)
        for (t in tuples) {
            println(t.joinToString(","))
        }
    }


    fun generate_insert_daily_cases(db: String, table: Table, tableYa: Table, createSqlPrefix: String, csvTuplesPrefix: String, casePrefix: String) {
        create_table(db, table)
        create_table(db, tableYa)
        val createSqlPath = "$createSqlPrefix/${table.tableName}.sql"
        Util.createFile(File(createSqlPath), table.sql())
        val createYaSqlPath = "$createSqlPrefix/${tableYa.tableName}.sql"
        Util.createFile(File(createYaSqlPath), tableYa.sql())

        val orcPath = "$db.${table.tableName}.orc"
        OrcUtil.createOrcFile(orcPath, table.keyFields(), table.valueFields(setOf()), 100, 4096)
        val fieldNames = table.fields.map { it.name }.toTypedArray()
        val tuples = OrcUtil.orcToList(orcPath, *fieldNames)

        val csvTablesPath = "$csvTuplesPrefix/${table.tableName}.csv"
        Util.createFile(File(csvTablesPath), tuples.joinToString("\n") { it.joinToString(", ") })

        val sql = table.insertIntoValuesSql(tuples)
        execute(db, sql)

        val sqlYa = tableYa.insertIntoSubQuerySql(table.selectAll())
        execute(db, sqlYa)

        val fp = fingerprint_murmur_hash3_32(db, table.selectAll())
        //val fpYa = fingerprint_murmur_hash3_32(db, tableYa.selectAll())
        //if (fp!=fpYa) {
        //    compare_each_column(db, table.tableName, tableYa.tableName, table.fields.map { it.name })
        //}
        //Assert.assertEquals(fp, fpYa)
        val case = Util.renderTemplate("insert_into_daily.template",
                "test" to table.tableName,
                "name" to table.tableName,
                "fields" to table.fields.map { it.name },
                "fingerprint" to fp)
        val casePath = "$casePrefix/${table.tableName}"
        Util.createFile(File(casePath), case)
    }

    @Test
    fun test_not_nullable_duplicate_table() {
        create_db(db)
        val createSqlPrefix = "create_sql_dir"
        val dataCsvPrefix = "create_csv_dir"
        val casesPrefix = "cases_dir"
        File(createSqlPrefix).mkdirs()
        File(dataCsvPrefix).mkdirs()
        File(casesPrefix).mkdirs()
        val tables = listOf(
                table1.notNullableTable().renameTable("not_nullable_duplicate_table"),
                table1.nullableTable().renameTable("nullable_duplicate_table"),
                table1.notNullableTable().aggregateTable(listOf(AggregateType.MAX)).renameTable("not_nullable_aggregate_max_table"),
                table1.notNullableTable().aggregateTable(listOf(AggregateType.MIN)).renameTable("not_nullable_aggregate_min_table"),
                table1.notNullableTable().aggregateTable(listOf(AggregateType.SUM)).renameTable("not_nullable_aggregate_sum_table"),
                table1.notNullableTable().aggregateTable(listOf(AggregateType.REPLACE)).renameTable("not_nullable_aggregate_replace_table"),
                table1.notNullableTable().aggregateTable(listOf(AggregateType.REPLACE_IF_NOT_NULL)).renameTable("not_nullable_aggregate_replace_if_not_null_table"),
                table1.notNullableTable().uniqueTable().renameTable("not_nullable_unique_table"),
                table1.nullableTable().uniqueTable().renameTable("nullable_unique_table"),

                table1.nullableTable().aggregateTable(listOf(AggregateType.MAX)).renameTable("nullable_aggregate_max_table"),
                table1.nullableTable().aggregateTable(listOf(AggregateType.MIN)).renameTable("nullable_aggregate_min_table"),
                table1.nullableTable().aggregateTable(listOf(AggregateType.SUM)).renameTable("nullable_aggregate_sum_table"),
                table1.nullableTable().aggregateTable(listOf(AggregateType.REPLACE)).renameTable("nullable_aggregate_replace_table"),
                table1.nullableTable().aggregateTable(listOf(AggregateType.REPLACE_IF_NOT_NULL)).renameTable("nullable_aggregate_replace_if_not_null_table")

                //table1.nullableTable().aggregateTable(listOf(AggregateType.BITMAP_UNION)).renameTable("nullable_aggregate_bitmap_union_table"),
                //table1.nullableTable().aggregateTable(listOf(AggregateType.HLL_UNION)).renameTable("nullable_aggregate_hll_union_table"),
                //table1.nullableTable().aggregateTable(listOf(AggregateType.PERCENTILE_UNION)).renameTable("nullable_aggregate_percentile_union_table"),

                //table1.notNullableTable().aggregateTable(listOf(AggregateType.BITMAP_UNION)).renameTable("not_nullable_aggregate_bitmap_union_table"),
                //table1.notNullableTable().aggregateTable(listOf(AggregateType.HLL_UNION)).renameTable("not_nullable_aggregate_hll_union_table"),
                //table1.notNullableTable().aggregateTable(listOf(AggregateType.PERCENTILE_UNION)).renameTable("not_nullable_aggregate_percentile_union_table")

                )

        val tablePairs = tables.map{it to it.renameTable(it.tableName+"_ya")}
        tablePairs.forEach {(table, tableYa)->
            generate_insert_daily_cases(db, table, tableYa, createSqlPrefix, dataCsvPrefix, casesPrefix)
        }
    }

    @Test
    fun test_create_table_sql() {
        val table1 = table1.nullableTable().renameTable("nullable_table")
        val table2 = table1.nullableTable().aggregateTable(listOf(AggregateType.SUM)).renameTable("nullable_aggregate_sum_table")
        println(table1.sql())
        println(table2.sql())
    }
}