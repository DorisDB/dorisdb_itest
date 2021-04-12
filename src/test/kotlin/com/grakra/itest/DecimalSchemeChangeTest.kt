package com.grakra.itest

import com.grakra.TestMethodCapture
import com.grakra.schema.*
import com.grakra.util.RandUtil
import com.grakra.util.Util
import org.testng.Assert
import org.testng.annotations.AfterClass
import org.testng.annotations.BeforeClass
import org.testng.annotations.Listeners
import org.testng.annotations.Test
import java.io.File
import java.io.PrintStream

@Listeners(TestMethodCapture::class)
class DecimalSchemeChangeTest : DorisDBRemoteITest() {

    fun schema_change(db: String, tableName: String, originalField: Field, changedField: Field, gen: () -> Any) {
        val fields = listOf(
                SimpleField.fixedLength("col_int", FixedLengthType.TYPE_INT),
                originalField)
        val table = Table(tableName, fields, 1)
        val tableMirror = table.renameTable("${table.tableName}_mirror")

        val idGen = Util.generateCounter()
        create_db(db)
        create_table(db, table)
        insert_values(db, table, 100, originalField.name to gen, "col_int" to idGen)
        create_table(db, tableMirror)
        insert_select(db, tableMirror, table)

        //query_print(db, table.selectAll())
        val (stmt, newTable) = table.alterTable(db, changedField)
        execute(db, stmt!!)
        check_alter_table_finished(db, table)
        query_print(db, "select ${table.tableName}.col_int, ${table.tableName}.${changedField.name}, ${tableMirror.tableName}.${originalField.name}" +
                " from ${table.tableName} JOIN ${tableMirror.tableName} ON ${table.tableName}.col_int = ${tableMirror.tableName}.col_int")
        val fp0 = fingerprint_murmur_hash3_32(db, "select ${changedField.name} from ${table.tableName}")
        val fp1 = fingerprint_murmur_hash3_32(db,
                "select cast(${originalField.name} as ${changedField.sqlType()}) as ${originalField.name} from ${tableMirror.tableName}")
        Assert.assertEquals(fp0, fp1)
    }

    val genCodeFile = "decimal_schema_change.py"
    var genCode: PrintStream? = null
    // val createSqlPrefix = "decimal_schema_change/sql"
    // val dataCsvPrefix = "decimal_schema_change/csv"

    @BeforeClass
    fun setUp() {
        val genCodeFileObj = File(genCodeFile)
        if (genCodeFileObj.exists()) {
            genCodeFileObj.delete()
        }
        genCode = PrintStream(genCodeFileObj.outputStream())
        // Util.recreateDir(createSqlPrefix)
        // Util.recreateDir(dataCsvPrefix)
    }

    @AfterClass
    fun tearDown() {
        genCode!!.flush()
        genCode!!.close()
    }


    fun schema_change_daily(db: String, tableName: String, originalField: Field, changedField: Field, gen: () -> Any) {
        val fields = listOf(
                SimpleField.fixedLength("col_int", FixedLengthType.TYPE_INT),
                originalField)
        val table = Table(tableName, fields, 1)
        val tableMirror = table.renameTable("${table.tableName}_mirror")
        val sqlCreateDb = create_db_sql(db)
        val sqlDropDb = drop_db_sql(db)
        val sqlTable = table.sql()
        val sqlTableMirror = tableMirror.sql()

        val idGen = Util.generateCounter()
        //create_db(db)
        //create_table(db, table)
        val sqlInsertValues = insert_values_sql(db, table, 100, originalField.name to gen, "col_int" to idGen)
        // create_table(db, tableMirror)
        // insert_select(db, tableMirror, table)
        val sqlInsertSelect = tableMirror.insertIntoSubQuerySql(table.selectAll())
        //query_print(db, table.selectAll())
        val (sqlAlterTable, newTable) = table.alterTable(db, changedField)
        // execute(db, sqlAlterTable!!)
        // check_alter_table_finished(db, table)
        val sqlShowAlterTable = table.showAlterTableColumnSql()

        // query_print(db, "select ${table.tableName}.col_int, ${table.tableName}.${changedField.name}, ${tableMirror.tableName}.${originalField.name}" +
        //        " from ${table.tableName} JOIN ${tableMirror.tableName} ON ${table.tableName}.col_int = ${tableMirror.tableName}.col_int")
        val sqlFp0 = fingerprint_murmur_hash3_32_sql(db, "select ${changedField.name} from ${table.tableName}")
        val sqlFp1 = fingerprint_murmur_hash3_32_sql(db,
                "select cast(${originalField.name} as ${changedField.sqlType()}) as ${originalField.name} from ${tableMirror.tableName}")
        Util.renderTemplate("schema_change.template",
                "sqlDropDb" to sqlDropDb,
                "sqlCreateDb" to sqlCreateDb,
                "sqlTable" to sqlTable,
                "sqlTableMirror" to sqlTableMirror,
                "sqlInsertValues" to sqlInsertValues,
                "sqlInsertSelect" to sqlInsertSelect,
                "sqlAlterTable" to sqlAlterTable,
                "sqlShowAlterTable" to sqlShowAlterTable,
                "sqlFp0" to sqlFp0,
                "sqlFp1" to sqlFp1).let {
            genCode!!.println(it)
            genCode!!.println()
        }
    }

    @Test
    fun testVarcharToDecimal128p20s3SchemaChange() {
        val originalField = SimpleField.varchar("col_decimal", 50)
        val changedField = SimpleField.decimal("col_decimal", 128, 20, 3)
        val decimalGen = RandUtil.generateRandomDecimal(20, 3, 0)
        val stringDecimalGen = {
            decimalGen().toPlainString().toByteArray()
        }
        schema_change("varchar_to_decimal128p20s3_db", "test_table", originalField, changedField, stringDecimalGen)
    }

    @Test
    fun testVarcharToDecimal64p15s10SchemaChange() {
        val originalField = SimpleField.varchar("col_decimal", 50)
        val changedField = SimpleField.decimal("col_decimal", 64, 15, 10)
        val decimalGen = RandUtil.generateRandomDecimal(15, 10, 0)
        val stringDecimalGen = {
            decimalGen().toPlainString().toByteArray()
        }
        schema_change("varchar_to_decimal64p15s10_db", "test_table", originalField, changedField, stringDecimalGen)
    }

    @Test
    fun testVarcharToDecimal32p7s2SchemaChange() {
        val originalField = SimpleField.varchar("col_decimal", 50)
        val changedField = SimpleField.decimal("col_decimal", 32, 7, 2)
        val decimalGen = RandUtil.generateRandomDecimal(7, 2, 0)
        val stringDecimalGen = {
            decimalGen().toPlainString().toByteArray()
        }
        schema_change("varchar_to_decimal32p7s2_db", "test_table", originalField, changedField, stringDecimalGen)
    }

    @Test
    fun testDecimal128p33s27VarcharSchemaChange() {
        val originalField = SimpleField.decimal("col_decimal", 128, 33, 27)
        val changedField = SimpleField.varchar("col_decimal", 50)
        val decimalGen = RandUtil.generateRandomDecimal(33, 27, 0)
        schema_change("decimal128p33s27_to_varchar_db", "test_table", originalField, changedField, decimalGen)
    }

    @Test
    fun testDecimal64p13s1VarcharSchemaChange() {
        val originalField = SimpleField.decimal("col_decimal", 64, 13, 1)
        val changedField = SimpleField.varchar("col_decimal", 50)
        val decimalGen = RandUtil.generateRandomDecimal(13, 1, 0)
        schema_change("decimal64p13s1_to_varchar_db", "test_table", originalField, changedField, decimalGen)
    }

    @Test
    fun testDecimal32p9s5VarcharSchemaChange() {
        val originalField = SimpleField.decimal("col_decimal", 32, 9, 5)
        val changedField = SimpleField.varchar("col_decimal", 50)
        val decimalGen = RandUtil.generateRandomDecimal(9, 5, 0)
        schema_change("decimal32p9s5_to_varchar_db", "test_table", originalField, changedField, decimalGen)
    }

    @Test
    fun testDecimalV2ToDecimal128p27s9SchemaChange() {
        val originalField = SimpleField.decimalv2("col_decimal", 27, 9)
        val changedField = SimpleField.decimal("col_decimal", 128, 27, 9)
        val decimalGen = RandUtil.generateRandomDecimal(27, 9, 0)
        schema_change("decimalv2_to_decimal128_db", "test_table", originalField, changedField, decimalGen)
    }

    @Test
    fun testDecimal32p7s2ToDecimal64p17s12SchemaChange() {
        val originalField = SimpleField.decimal("col_decimal", 32, 7, 2)
        val changedField = SimpleField.decimal("col_decimal", 64, 17, 12)
        val decimalGen = RandUtil.generateRandomDecimal(7, 2, 0)
        schema_change("decimal32p7s2_to_decimal64p17s12_db", "test_table", originalField, changedField, decimalGen)
    }

    @Test
    fun testDecimal32p7s2ToDecimal64p17s9SchemaChange() {
        val originalField = SimpleField.decimal("col_decimal", 32, 7, 2)
        val changedField = SimpleField.decimal("col_decimal", 64, 17, 9)
        val decimalGen = RandUtil.generateRandomDecimal(7, 2, 0)
        schema_change("decimal32p7s2_to_decimal64p17s9_db", "test_table", originalField, changedField, decimalGen)
    }

    @Test
    fun testDecimal32p7s2ToDecimal128p27s20SchemaChange() {
        val originalField = SimpleField.decimal("col_decimal", 32, 7, 2)
        val changedField = SimpleField.decimal("col_decimal", 128, 27, 20)
        val decimalGen = RandUtil.generateRandomDecimal(7, 2, 0)
        schema_change("decimal32p7s2_to_decimal128p27s20_db", "test_table", originalField, changedField, decimalGen)
    }

    @Test
    fun testDecimal64p15s10ToDecimal128p27s20SchemaChange() {
        val originalField = SimpleField.decimal("col_decimal", 64, 15, 10)
        val changedField = SimpleField.decimal("col_decimal", 128, 27, 20)
        val decimalGen = RandUtil.generateRandomDecimal(7, 2, 0)
        schema_change("decimal64p15s10_to_decimal128p27s20_db", "test_table", originalField, changedField, decimalGen)
    }

    @Test
    fun testDecimal128p7s2ToDecimal32p9s3SchemaChange() {
        val originalField = SimpleField.decimal("col_decimal", 128, 7, 2)
        val changedField = SimpleField.decimal("col_decimal", 32, 9, 3)
        val decimalGen = RandUtil.generateRandomDecimal(7, 2, 0)
        schema_change("decimal128p7s2_to_decimal32p9s3_db", "test_table", originalField, changedField, decimalGen)
    }

    @Test
    fun testDecimal128p7s2ToDecimal64p18s4SchemaChange() {
        val originalField = SimpleField.decimal("col_decimal", 128, 7, 2)
        val changedField = SimpleField.decimal("col_decimal", 64, 18, 4)
        val decimalGen = RandUtil.generateRandomDecimal(7, 2, 0)
        schema_change("decimal128p7s2_to_decimal64p18s4_db", "test_table", originalField, changedField, decimalGen)
    }

    @Test
    fun testDecimal64p7s2ToDecimal32p9s3SchemaChange() {
        val originalField = SimpleField.decimal("col_decimal", 64, 7, 2)
        val changedField = SimpleField.decimal("col_decimal", 32, 9, 3)
        val decimalGen = RandUtil.generateRandomDecimal(7, 2, 0)
        schema_change("decimal64p7s2_to_decimal32p9s3_db", "test_table", originalField, changedField, decimalGen)
    }

    @Test
    fun testDecimal32p7s2ToDecimal32p9s3SchemaChange() {
        val originalField = SimpleField.decimal("col_decimal", 32, 7, 2)
        val changedField = SimpleField.decimal("col_decimal", 32, 9, 3)
        val decimalGen = RandUtil.generateRandomDecimal(7, 2, 0)
        schema_change("decimal32p7s2_to_decimal32p9s3_db", "test_table", originalField, changedField, decimalGen)
    }

    @Test
    fun testDecimal64p14s5ToDecimal64p17s7chemaChange() {
        val originalField = SimpleField.decimal("col_decimal", 64, 14, 5)
        val changedField = SimpleField.decimal("col_decimal", 64, 17, 7)
        val decimalGen = RandUtil.generateRandomDecimal(14, 5, 0)
        schema_change("decimal64p14s5_to_decimal64p17s7_db", "test_table", originalField, changedField, decimalGen)
    }

    @Test
    fun testDecimal128p20s9ToDecimal128p38s20chemaChange() {
        val originalField = SimpleField.decimal("col_decimal", 128, 20, 9)
        val changedField = SimpleField.decimal("col_decimal", 128, 38, 20)
        val decimalGen = RandUtil.generateRandomDecimal(20, 9, 0)
        schema_change("decimal128p20s9_to_decimal128p38s20_db", "test_table", originalField, changedField, decimalGen)
    }
}