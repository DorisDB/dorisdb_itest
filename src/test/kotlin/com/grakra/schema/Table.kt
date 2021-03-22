package com.grakra.schema

import com.google.common.base.Preconditions
import com.grakra.util.Util

class Table(val tableName: String, val fields: List<Field>, val keyLimit: Int) {
    fun sql(): String {
        val fieldDefs = fields.map { it.sql() }
        val keys = fields.take(keyLimit).map { it.name }
        return Util.renderTemplate("duplicate_table.template",
                "tableName" to tableName,
                "fieldDefs" to fieldDefs,
                "keys" to keys,
                "replicaFactor" to 1)

    }

    fun hiveSql(format: String, partitionKeys: Array<String>, clusterKeys: Array<String>, sortKeys: Array<String>, numBuckets: Int): String {
        val fieldsMap = fields.map { e -> e.name to e }.toMap()
        val partitionFields = partitionKeys.map { k -> fieldsMap.getValue(k) }
        val partitionKeySet = partitionKeys.toSet()
        val nonPartitionFields = fields.filter { f -> !partitionKeySet.contains(f.name) }
        Preconditions.checkState(clusterKeys.none { n -> !fieldsMap.containsKey(n) })
        Preconditions.checkState(sortKeys.none { n -> !fieldsMap.containsKey(n) })
        return Util.renderTemplate("hive_${format}_table.template",
                "tableName" to "${format}_$tableName",
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
        val columns = fields.map{it.name}
        return Util.renderTemplate("insert_into_values.template",
                "tableName" to tableName,
                "columns" to columns,
                "tuples" to tuples)
    }

    fun insertIntoSubQuerySql(subQuery:String):String{
        val columns = fields.map{it.name}
        return Util.renderTemplate("insert_into_subquery.template",
                "tableName" to tableName,
                "columns" to columns,
                "subQuery" to subQuery)
    }
}