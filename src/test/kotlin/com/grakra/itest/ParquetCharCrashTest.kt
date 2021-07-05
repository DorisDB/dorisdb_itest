package com.grakra.itest

import com.grakra.TestMethodCapture
import com.grakra.schema.Table
import com.grakra.tables.Tables
import com.grakra.util.HiveClient
import org.testng.annotations.Listeners
import org.testng.annotations.Test

@Listeners(TestMethodCapture::class)
class ParquetCharCrashTest : DorisDBRemoteITest() {
    @Test
    fun orc_to_parquet_via_hive() {
        val orcFiles = arrayOf(
                "/rpf/varchar300_csv/varchar300_0001.csv",
                "/rpf/varchar300_csv/varchar300_0002.csv"
        )

        val hiveClient = HiveClient("127.0.0.1:10000/default", "grakra", "")
        Tables.varchar300_table.createHiveParquetScripts(hiveClient, orcFiles, "csv")
    }

    @Test
    fun load_test0(){
        val db = "load_test0"
        create_db(db)
        create_table(db, Tables.char_table)
        broker_load(db, Tables.char_table, "/user/hive/warehouse/parquet_char_table/*")
    }

    @Test
    fun load_test1(){
        val db = "load_test1"
        create_db(db)
        admin_set_vectorized_load_enable(true)
        create_table(db, Tables.char_table2)
        broker_load(db, Tables.char_table2, "/user/hive/warehouse/parquet_char_table/*")
    }

    @Test
    fun load_test2(){
        val db = "load_test2"
        create_db(db)
        admin_set_vectorized_load_enable(true)
        create_table(db, Tables.varchar300_table)
        broker_load(db, Tables.varchar300_table, "/user/hive/warehouse/parquet_varchar300_table/*")
    }

    @Test
    fun load_test3(){
        val db = "load_test3"
        create_db(db)
        admin_set_vectorized_load_enable(true)
        create_table(db, Tables.varchar300_table2)
        broker_load(db, Tables.varchar300_table2, "/user/hive/warehouse/parquet_varchar300_table/*")
    }


    @Test
    fun load_test4(){
        val db = "load_test4"
        create_db(db)
        admin_set_vectorized_load_enable(true)
        create_table(db, Tables.varchar300_table3)
        broker_load(db, Tables.varchar300_table3, "/user/hive/warehouse/parquet_varchar300_table/*")
    }
}