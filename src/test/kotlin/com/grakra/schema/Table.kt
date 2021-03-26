package com.grakra.schema

import com.google.common.base.Preconditions
import com.grakra.dorisdb.HiveClient
import com.grakra.util.Util

enum class TableType {
    DUPLICATE_TABLE,
    AGGREGATE_TABLE,
    UNIQUE_TABLE,
}

class Table(val tableName: String, val fields: List<Field>, val keyLimit: Int, val tableType: TableType = TableType.DUPLICATE_TABLE) {
    fun sql(): String {
        val fieldDefs = fields.map { it.sql() }
        val keys = fields.take(keyLimit).map { it.name }

        val template = tableType.name.toLowerCase() + ".template"
        return Util.renderTemplate(template,
                "tableName" to tableName,
                "fieldDefs" to fieldDefs,
                "keys" to keys,
                "replicaFactor" to 1)

    }

    fun selectAll() = "select * from $tableName"

    fun brokerLoadSql(db: String, format: String, hdfsPath: String): String {
        return Util.renderTemplate("broker_load.sql.template",
                "db" to db,
                "table" to tableName,
                "labelId" to System.currentTimeMillis().toString(),
                "hdfsPath" to hdfsPath,
                "format" to format,
                "columnList" to (keyFields() + valueFields(emptySet())).map { it.name })
    }

    fun createOrcFile(prefix: String, numRows: Int) {
        val path = "$prefix/${tableName}_$numRows.orc"
        OrcUtil.createOrcFile(path, keyFields(), valueFields(emptySet()), numRows, 4096)
    }

    fun createHiveParquetScripts(hiveClient: HiveClient, fromFiles: Array<String>, fromFormat: String) {
        val dryRun = true
        val toFormat = "parquet"
        hiveClient.q { hive ->
            hive.e(dropHiveTableSql(fromFormat), dryRun)
            hive.e(dropHiveTableSql(toFormat), dryRun)
            hive.e(createHiveTableSql(fromFormat), dryRun)
            hive.e(createHiveTableSql(toFormat), dryRun)
        }
        hiveClient.q { hive ->
            fromFiles.forEach { file ->
                val fromTableName = hiveTableName(fromFormat)
                val toTableName = hiveTableName(toFormat)
                val loadCSVSql = "LOAD DATA INPATH 'hdfs://$file'  INTO TABLE $fromTableName"
                val insertParquetSql = "INSERT INTO $toTableName select * from $fromTableName"
                hive.e(dropHiveTableSql(fromFormat), dryRun)
                hive.e(createHiveTableSql(fromFormat), dryRun)
                hive.e(loadCSVSql, dryRun)
                hive.e(insertParquetSql, dryRun)
            }
        }
    }

    fun hiveTableName(format: String) = "${format}_$tableName"
    fun dropHiveTableSql(format: String) = "DROP TABLE IF EXISTS ${hiveTableName(format)}"
    fun createHiveTableSql(format: String) = hiveSql(format, arrayOf(), arrayOf(), arrayOf(), 1)
    fun hiveSql(format: String, partitionKeys: Array<String>, clusterKeys: Array<String>, sortKeys: Array<String>, numBuckets: Int): String {
        val fieldsMap = fields.map { e -> e.name to e }.toMap()
        val partitionFields = partitionKeys.map { k -> fieldsMap.getValue(k) }
        val partitionKeySet = partitionKeys.toSet()
        val nonPartitionFields = fields.filter { f -> !partitionKeySet.contains(f.name) }
        Preconditions.checkState(clusterKeys.none { n -> !fieldsMap.containsKey(n) })
        Preconditions.checkState(sortKeys.none { n -> !fieldsMap.containsKey(n) })
        return Util.renderTemplate("hive_${format}_table.template",
                "tableName" to hiveTableName(format),
                "non_partition_fields" to nonPartitionFields.map { f -> f.hiveSql() },
                "partition_fields" to if (partitionFields.isEmpty()) {
                    null
                } else {
                    partitionFields.map { f -> f.simple().hivePartitionKeySql() }
                },
                "cluster_keys" to if (clusterKeys.isEmpty()) {
                    null
                } else {
                    clusterKeys
                },
                "sort_keys" to if (sortKeys.isEmpty()) {
                    null
                } else {
                    sortKeys
                },
                "num_buckets" to numBuckets)
    }

    fun keyFields() = fields.take(keyLimit) as List<SimpleField>
    fun valueFields(excludes: Set<String>) = fields.drop(keyLimit).filter { !excludes.contains(it.name) }
    fun insertIntoValuesSql(tuples: List<List<String>>): String {
        val columns = fields.map { it.name }
        return Util.renderTemplate("insert_into_values.template",
                "tableName" to tableName,
                "columns" to columns,
                "tuples" to tuples)
    }

    fun insertIntoSubQuerySql(subQuery: String): String {
        val columns = fields.map { it.name }
        return Util.renderTemplate("insert_into_subquery.template",
                "tableName" to tableName,
                "columns" to columns,
                "subQuery" to subQuery)
    }

    fun nullableTable(): Table {
        val toNullable: (Field) -> Field = { f ->
            when (f) {
                is SimpleField -> CompoundField.nullable(f, 50)
                is CompoundField -> CompoundField.nullable(f.fld, 50)
                is AggregateField -> AggregateField(CompoundField.nullable(f.fld.fld, 50), f.aggType)
            }
        }
        return Table(tableName, fields.map(toNullable), keyLimit, tableType)
    }

    fun notNullableTable(): Table {
        val toNotNullable: (Field) -> Field = { f ->
            when (f) {
                is SimpleField -> f
                is CompoundField -> f.fld
                is AggregateField -> AggregateField.aggregate(CompoundField.trivial(f.fld.fld), f.aggType)
            }
        }
        return Table(tableName, fields.map(toNotNullable), keyLimit, tableType)
    }

    fun aggregateTable(aggTypes: List<AggregateType>): Table {
        val nextAggType = Util.roundRobin(aggTypes)
        val toReplace: (Field) -> Field = { f ->
            when (f) {
                is SimpleField -> AggregateField.aggregate(CompoundField.trivial(f), nextAggType())
                is CompoundField -> AggregateField.aggregate(f, nextAggType())
                is AggregateField -> AggregateField.aggregate(f.fld, nextAggType())
            }
        }

        val toNoneAggregate: (Field) -> Field = { f ->
            when (f) {
                is AggregateField -> f.fld
                else -> f
            }
        }

        return Table(tableName, keyFields().map(toNoneAggregate) + valueFields(emptySet()).map(toReplace), keyLimit, TableType.AGGREGATE_TABLE)
    }

    fun uniqueTable(): Table {
        val toNoneAggregate: (Field) -> Field = { f ->
            when (f) {
                is AggregateField -> f.fld
                else -> f
            }
        }
        return Table(tableName, fields.map(toNoneAggregate), keyLimit, TableType.UNIQUE_TABLE)
    }

    fun resetKeyLimit(newKeyLimit: Int) = Table(tableName, fields, newKeyLimit, tableType)

    fun renameTable(newTableName: String) = Table(newTableName, fields, keyLimit, tableType)
}