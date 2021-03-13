package com.grakra.schema

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

    fun keyFields() = fields.take(keyLimit) as List<SimpleField>
    fun valueFields(excludes:Set<String>) = fields.drop(keyLimit).filter{!excludes.contains(it.name)}
}