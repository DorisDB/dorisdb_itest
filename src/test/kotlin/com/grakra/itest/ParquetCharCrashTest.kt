package com.grakra.itest

import com.grakra.TestMethodCapture
import com.grakra.tables.Tables
import com.grakra.util.HiveClient
import org.testng.annotations.Listeners
import org.testng.annotations.Test

@Listeners(TestMethodCapture::class)
class ParquetCharCrashTest : DorisDBRemoteITest() {
    @Test
    fun orc_to_parquet_via_hive() {
        val orcFiles = arrayOf(
                "/rpf/char_tables/char_table_data_0001.orc",
                "/rpf/char_tables/char_table_data_0002.orc"
        )
        val hiveClient = HiveClient("127.0.0.1:10000/default", "grakra", "")
        Tables.char_table.createHiveParquetScripts(hiveClient, orcFiles, "orc")
    }
}