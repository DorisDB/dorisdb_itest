package com.grakra.itest

import com.grakra.TestMethodCapture
import com.grakra.schema.FixedLengthType
import com.grakra.schema.SimpleField
import com.grakra.schema.Table
import com.grakra.util.Util
import org.testng.Assert
import org.testng.annotations.Listeners
import org.testng.annotations.Test
import sun.java2d.pipe.SpanShapeRenderer
import java.io.File

@Listeners(TestMethodCapture::class)
class TpcdsTest:DorisDBRemoteITest(){
    //@Test
    fun remove_database(){
        show_databases().filter {
            it.startsWith("decimal_v3_load") ||
                    it.startsWith("g") ||
                    it.startsWith("doris_decimal_cast_test") ||
                    it.startsWith("doris_insert_test") ||
                    it.startsWith("doris_decimal")
        }.forEach{drop_database(it)}
    }

    val v2_db= "tpcds_v2_db"
    val v3_db= "tpcds_v3_db"

    //@Test
    fun create_tables(){
        val sqlScript = Util.readContentFromResource("tpcds_create.sql")
        println(sqlScript)
        create_db(v2_db)
        create_db(v3_db)
        admin_set_enable_decimal_v3(false)
        execute(v2_db, sqlScript)
        admin_set_enable_decimal_v3(true)
        execute(v3_db, sqlScript)
    }
    //@Test
    fun query() {
        Util.listResource("tpcds"){true}.forEach{it->println(it.canonicalPath)}
    }

    @Test
    fun query65() {
        //val q = Util.readContentFromResource("tpcds/query65.sql")
        //val q= " \tselect ss_store_sk, avg(revenue) as ave\n" +
        //        " \tfrom\n" +
        //        " \t    (select  ss_store_sk, ss_item_sk, \n" +
        //        " \t\t     sum(ss_sales_price) as revenue\n" +
        //        " \t\tfrom store_sales, date_dim\n" +
        //        " \t\twhere ss_sold_date_sk = d_date_sk and d_month_seq between 1176 and 1176+11\n" +
        //        " \t\tgroup by ss_store_sk, ss_item_sk) sa\n" +
        //        " \tgroup by ss_store_sk"
        val q = "select\n" +
                "\tsc.revenue,\n" +
                "\tsb.ave,\n" +
                "\t(0.1 * sb.ave) as ave2,\n" +
                "\t(sc.revenue <= 0.1 * sb.ave) as abc\n"
                "from store, item,\n" +
                "     (select ss_store_sk, avg(revenue) as ave\n" +
                " \tfrom\n" +
                " \t    (select  ss_store_sk, ss_item_sk, \n" +
                " \t\t     sum(ss_sales_price) as revenue\n" +
                " \t\tfrom store_sales, date_dim\n" +
                " \t\twhere ss_sold_date_sk = d_date_sk and d_month_seq between 1176 and 1176+11\n" +
                " \t\tgroup by ss_store_sk, ss_item_sk) sa\n" +
                " \tgroup by ss_store_sk) sb,\n" +
                "     (select  ss_store_sk, ss_item_sk, sum(ss_sales_price) as revenue\n" +
                " \tfrom store_sales, date_dim\n" +
                " \twhere ss_sold_date_sk = d_date_sk and d_month_seq between 1176 and 1176+11\n" +
                " \tgroup by ss_store_sk, ss_item_sk) sc\n" +
                " where sb.ss_store_sk = sc.ss_store_sk and \n" +
                "       sc.revenue <= 0.1 * sb.ave and\n" +
                "       s_store_sk = sc.ss_store_sk and\n" +
                "       i_item_sk = sc.ss_item_sk\n" +
                " order by s_store_name, i_item_desc"
        //val fpV2 = fingerprint_murmur_hash3_32(v2_db, q)
        //val fpV3 = fingerprint_murmur_hash3_32(v3_db, q)
        //Assert.assertEquals(fpV2,fpV3)
        val rs2 = query(v2_db, q)

        Util.enclosedOutputStream(File("rs2")) {printer->
            rs2!!.forEach { rows ->
                rows.entries.joinToString { (k, v) -> "$k=$v" }.let { printer.println(it) }
            }
        }
        val rs3 = query(v3_db, q)
        Util.enclosedOutputStream(File("rs3")) {printer->
            rs3!!.forEach { rows ->
                rows.entries.joinToString { (k, v) -> "$k=$v" }.let { printer.println(it) }
            }
        }
    }

    @Test
    fun create_table(){
        val table = Table("test_compare_table", listOf(
                SimpleField.fixedLength("c0", FixedLengthType.TYPE_INT),
                SimpleField.decimal("c1", 64, 7,2),
                SimpleField.decimal("c2", 64, 8,3),
                SimpleField.decimal("c3", 64, 9,2)
        ), 1)
        create_db("test_compare_db");
        create_table("test_compare_db", table)
    }
}