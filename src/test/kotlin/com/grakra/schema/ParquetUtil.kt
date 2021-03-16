package com.grakra.schema

import com.grakra.util.Util

object ParquetUtil {
    fun createParquetBrokerLoadSql(db: String, table: Table, hdfsPath: String): String {
        return Util.renderTemplate("broker_load.sql.template",
                "db" to db,
                "table" to table.tableName,
                "labelId" to System.currentTimeMillis().toString(),
                "hdfsPath" to hdfsPath,
                "format" to "parquet",
                "columnList" to (table.keyFields() + table.valueFields(emptySet())).map { it.name })
    }
}