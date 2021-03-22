package com.grakra.itest

import com.grakra.TestMethodCapture
import com.grakra.dorisdb.HiveClient
import org.testng.Assert
import org.testng.annotations.Listeners
import org.testng.annotations.Test

@Listeners(TestMethodCapture::class)
class VectorizedPerformanceTest : DorisDBRemoteITest() {
    val db = "vectorized_parquet_load_db"
    val csv_table = "csv_web_sales"
    val parquet_table = "parquet_web_sales"
    val drop_hive_csv_table = "drop table if exists csv_web_sales"
    val drop_hive_parquet_table = "drop table if exists parquet_web_sales"
    val create_hive_csv_table = """
        create table csv_web_sales
        (
            ws_item_sk                integer               ,
            ws_order_number           integer               ,
            ws_sold_date_sk           integer                       ,
            ws_sold_time_sk           integer                       ,
            ws_ship_date_sk           integer                       ,
            ws_bill_customer_sk       integer                       ,
            ws_bill_cdemo_sk          integer                       ,
            ws_bill_hdemo_sk          integer                       ,
            ws_bill_addr_sk           integer                       ,
            ws_ship_customer_sk       integer                       ,
            ws_ship_cdemo_sk          integer                       ,
            ws_ship_hdemo_sk          integer                       ,
            ws_ship_addr_sk           integer                       ,
            ws_web_page_sk            integer                       ,
            ws_web_site_sk            integer                       ,
            ws_ship_mode_sk           integer                       ,
            ws_warehouse_sk           integer                       ,
            ws_promo_sk               integer                       ,
            ws_quantity               integer                       ,
            ws_wholesale_cost         decimal(7,2)                  ,
            ws_list_price             decimal(7,2)                  ,
            ws_sales_price            decimal(7,2)                  ,
            ws_ext_discount_amt       decimal(7,2)                  ,
            ws_ext_sales_price        decimal(7,2)                  ,
            ws_ext_wholesale_cost     decimal(7,2)                  ,
            ws_ext_list_price         decimal(7,2)                  ,
            ws_ext_tax                decimal(7,2)                  ,
            ws_coupon_amt             decimal(7,2)                  ,
            ws_ext_ship_cost          decimal(7,2)                  ,
            ws_net_paid               decimal(7,2)                  ,
            ws_net_paid_inc_tax       decimal(7,2)                  ,
            ws_net_paid_inc_ship      decimal(7,2)                  ,
            ws_net_paid_inc_ship_tax  decimal(7,2)                  ,
            ws_net_profit             decimal(7,2)
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '|'
        STORED AS TEXTFILE
    """.trimIndent()

    val create_hive_parquet_table = """
        create table parquet_web_sales
        (
            ws_item_sk                integer               ,
            ws_order_number           integer               ,
            ws_sold_date_sk           integer                       ,
            ws_sold_time_sk           integer                       ,
            ws_ship_date_sk           integer                       ,
            ws_bill_customer_sk       integer                       ,
            ws_bill_cdemo_sk          integer                       ,
            ws_bill_hdemo_sk          integer                       ,
            ws_bill_addr_sk           integer                       ,
            ws_ship_customer_sk       integer                       ,
            ws_ship_cdemo_sk          integer                       ,
            ws_ship_hdemo_sk          integer                       ,
            ws_ship_addr_sk           integer                       ,
            ws_web_page_sk            integer                       ,
            ws_web_site_sk            integer                       ,
            ws_ship_mode_sk           integer                       ,
            ws_warehouse_sk           integer                       ,
            ws_promo_sk               integer                       ,
            ws_quantity               integer                       ,
            ws_wholesale_cost         decimal(7,2)                  ,
            ws_list_price             decimal(7,2)                  ,
            ws_sales_price            decimal(7,2)                  ,
            ws_ext_discount_amt       decimal(7,2)                  ,
            ws_ext_sales_price        decimal(7,2)                  ,
            ws_ext_wholesale_cost     decimal(7,2)                  ,
            ws_ext_list_price         decimal(7,2)                  ,
            ws_ext_tax                decimal(7,2)                  ,
            ws_coupon_amt             decimal(7,2)                  ,
            ws_ext_ship_cost          decimal(7,2)                  ,
            ws_net_paid               decimal(7,2)                  ,
            ws_net_paid_inc_tax       decimal(7,2)                  ,
            ws_net_paid_inc_ship      decimal(7,2)                  ,
            ws_net_paid_inc_ship_tax  decimal(7,2)                  ,
            ws_net_profit             decimal(7,2)
        )
        STORED AS PARQUET
    """.trimIndent()

    val non_vectorized_table_name = "web_sales"
    val vectorized_table_name = "web_sales_vectorized"
    val create_non_vectorized_table_sql = """
        create table web_sales
        (
            ws_item_sk                integer                       ,
            ws_order_number           integer                       ,
            ws_sold_date_sk           integer                       ,
            ws_sold_time_sk           integer                       ,
            ws_ship_date_sk           integer                       ,
            ws_bill_customer_sk       integer                       ,
            ws_bill_cdemo_sk          integer                       ,
            ws_bill_hdemo_sk          integer                       ,
            ws_bill_addr_sk           integer                       ,
            ws_ship_customer_sk       integer                       ,
            ws_ship_cdemo_sk          integer                       ,
            ws_ship_hdemo_sk          integer                       ,
            ws_ship_addr_sk           integer                       ,
            ws_web_page_sk            integer                       ,
            ws_web_site_sk            integer                       ,
            ws_ship_mode_sk           integer                       ,
            ws_warehouse_sk           integer                       ,
            ws_promo_sk               integer                       ,
            ws_quantity               integer                       ,
            ws_wholesale_cost         decimal(7,2)                  ,
            ws_list_price             decimal(7,2)                  ,
            ws_sales_price            decimal(7,2)                  ,
            ws_ext_discount_amt       decimal(7,2)                  ,
            ws_ext_sales_price        decimal(7,2)                  ,
            ws_ext_wholesale_cost     decimal(7,2)                  ,
            ws_ext_list_price         decimal(7,2)                  ,
            ws_ext_tax                decimal(7,2)                  ,
            ws_coupon_amt             decimal(7,2)                  ,
            ws_ext_ship_cost          decimal(7,2)                  ,
            ws_net_paid               decimal(7,2)                  ,
            ws_net_paid_inc_tax       decimal(7,2)                  ,
            ws_net_paid_inc_ship      decimal(7,2)                  ,
            ws_net_paid_inc_ship_tax  decimal(7,2)                  ,
            ws_net_profit             decimal(7,2)
        )
        duplicate key (ws_item_sk, ws_order_number)
        distributed by hash(ws_item_sk, ws_order_number) buckets 1
        properties(
            "replication_num"="1"
        );
    """.trimIndent()

    val create_vectorized_table_sql = """
        create table web_sales_vectorized
        (
            ws_item_sk                integer                       ,
            ws_order_number           integer                       ,
            ws_sold_date_sk           integer                       ,
            ws_sold_time_sk           integer                       ,
            ws_ship_date_sk           integer                       ,
            ws_bill_customer_sk       integer                       ,
            ws_bill_cdemo_sk          integer                       ,
            ws_bill_hdemo_sk          integer                       ,
            ws_bill_addr_sk           integer                       ,
            ws_ship_customer_sk       integer                       ,
            ws_ship_cdemo_sk          integer                       ,
            ws_ship_hdemo_sk          integer                       ,
            ws_ship_addr_sk           integer                       ,
            ws_web_page_sk            integer                       ,
            ws_web_site_sk            integer                       ,
            ws_ship_mode_sk           integer                       ,
            ws_warehouse_sk           integer                       ,
            ws_promo_sk               integer                       ,
            ws_quantity               integer                       ,
            ws_wholesale_cost         decimal(7,2)                  ,
            ws_list_price             decimal(7,2)                  ,
            ws_sales_price            decimal(7,2)                  ,
            ws_ext_discount_amt       decimal(7,2)                  ,
            ws_ext_sales_price        decimal(7,2)                  ,
            ws_ext_wholesale_cost     decimal(7,2)                  ,
            ws_ext_list_price         decimal(7,2)                  ,
            ws_ext_tax                decimal(7,2)                  ,
            ws_coupon_amt             decimal(7,2)                  ,
            ws_ext_ship_cost          decimal(7,2)                  ,
            ws_net_paid               decimal(7,2)                  ,
            ws_net_paid_inc_tax       decimal(7,2)                  ,
            ws_net_paid_inc_ship      decimal(7,2)                  ,
            ws_net_paid_inc_ship_tax  decimal(7,2)                  ,
            ws_net_profit             decimal(7,2)
        )
        duplicate key (ws_item_sk, ws_order_number)
        distributed by hash(ws_item_sk, ws_order_number) buckets 1
        properties(
            "replication_num"="1"
        );
    """.trimIndent()

    @Test
    fun create_tables() {
        create_db(db)
        execute(db, create_vectorized_table_sql)
        execute(db, create_non_vectorized_table_sql)
        query_print(db, "desc $vectorized_table_name")
        query_print(db, "desc $non_vectorized_table_name")
    }

    @Test
    fun prepare_data() {
        val csvFiles = arrayOf(
                "/rpf/web_scales/web_sales_998_1000.dat",
                "/rpf/web_scales/web_sales_999_1000.dat",
                "/rpf/web_scales/web_sales_99_1000.dat",
                "/rpf/web_scales/web_sales_9_1000.dat"
        )
        val hiveClient = HiveClient("127.0.0.1:10000/default", "grakra", "")
        hiveClient.q { hive ->
            hive.e(drop_hive_csv_table)
            hive.e(drop_hive_parquet_table)
            hive.e(create_hive_csv_table)
            hive.e(create_hive_parquet_table)
        }
        hiveClient.q { hive ->
            csvFiles.forEach { csvFile ->
                val loadCSVSql = "LOAD DATA INPATH 'hdfs://$csvFile'  INTO TABLE $csv_table"
                val insertParquetSql = "INSERT INTO $parquet_table select * from $csv_table"
                hive.e(drop_hive_csv_table)
                hive.e(create_hive_csv_table)
                hive.e(loadCSVSql)
                hive.e(insertParquetSql)
            }
        }
    }

    val non_vectorized_load="""
        USE $db;
        LOAD LABEL $db.non_vectorized_${System.currentTimeMillis()} (
        DATA INFILE("hdfs://172.26.92.141:9002/user/hive/warehouse/parquet_web_sales/*")
        INTO TABLE `$non_vectorized_table_name`
        FORMAT AS "PARQUET"
        (ws_sold_date_sk,ws_sold_time_sk,ws_ship_date_sk,ws_item_sk,ws_bill_customer_sk,ws_bill_cdemo_sk,ws_bill_hdemo_sk,ws_bill_addr_sk,ws_ship_customer_sk,ws_ship_cdemo_sk,ws_ship_hdemo_sk,ws_ship_addr_sk,ws_web_page_sk,ws_web_site_sk,ws_ship_mode_sk,ws_warehouse_sk,ws_promo_sk,ws_order_number,ws_quantity,ws_wholesale_cost,ws_list_price,ws_sales_price,ws_ext_discount_amt,ws_ext_sales_price,ws_ext_wholesale_cost,ws_ext_list_price,ws_ext_tax,ws_coupon_amt,ws_ext_ship_cost,ws_net_paid,ws_net_paid_inc_tax,ws_net_paid_inc_ship,ws_net_paid_inc_ship_tax,ws_net_profit)
        )
        WITH BROKER hdfs ("username"="root", "password"="");
    """.trimIndent()

    val vectorized_load="""
        USE $db;
        LOAD LABEL $db.vectorized_${System.currentTimeMillis()} (
        DATA INFILE("hdfs://172.26.92.141:9002/user/hive/warehouse/parquet_web_sales/*")
        INTO TABLE `$vectorized_table_name`
        FORMAT AS "PARQUET"
        (ws_sold_date_sk,ws_sold_time_sk,ws_ship_date_sk,ws_item_sk,ws_bill_customer_sk,ws_bill_cdemo_sk,ws_bill_hdemo_sk,ws_bill_addr_sk,ws_ship_customer_sk,ws_ship_cdemo_sk,ws_ship_hdemo_sk,ws_ship_addr_sk,ws_web_page_sk,ws_web_site_sk,ws_ship_mode_sk,ws_warehouse_sk,ws_promo_sk,ws_order_number,ws_quantity,ws_wholesale_cost,ws_list_price,ws_sales_price,ws_ext_discount_amt,ws_ext_sales_price,ws_ext_wholesale_cost,ws_ext_list_price,ws_ext_tax,ws_coupon_amt,ws_ext_ship_cost,ws_net_paid,ws_net_paid_inc_tax,ws_net_paid_inc_ship,ws_net_paid_inc_ship_tax,ws_net_profit)
        )
        WITH BROKER hdfs ("username"="root", "password"="");
    """.trimIndent()
    @Test
    fun non_vectorized_load(){
        admin_set_vectorized_load_enable(false)
        broker_load(non_vectorized_load)
    }

    @Test
    fun vectorized_load(){
        admin_set_vectorized_load_enable(true)
        broker_load(vectorized_load)
    }
    @Test
    fun compare_fingerprint(){
        val fpNonVectorized= fingerprint_murmur_hash3_32(db, "select * from $non_vectorized_table_name")
        val fpVectorized= fingerprint_murmur_hash3_32(db, "select * from $vectorized_table_name")
        println("fpNonVectorized=${fpNonVectorized}")
        println("fpVectorized=${fpVectorized}")
        Assert.assertEquals(fpNonVectorized, fpVectorized)
    }
}