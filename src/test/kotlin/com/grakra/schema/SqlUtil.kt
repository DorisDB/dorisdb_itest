package com.grakra.schema

object SqlUtil {
    fun limit1(sql: String): String {
        return "select * from ($sql) as t limit 1"
    }

    fun fingerprint(hashFunc: String, colNames: List<String>, sql: String): String {
        val selectItems = colNames.joinToString("+") { n -> "ifnull(sum($hashFunc($n)), 0)" }
        return "select ($selectItems) as fingerprint  from ($sql) as t"
    }
}