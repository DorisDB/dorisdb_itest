package com.grakra.util

class MySQLClient(private val hostPort: String) {
    private var db2client: MutableMap<String, MySQLUtil.Sql>? by thread_local()
    private val nodbTag = "!!0xdeadbeef!!"
    fun <T> q(db: String, f: (MySQLUtil.Sql) -> T): T {
        if (db2client == null) {
            db2client = mutableMapOf()
        }
        val db2client = db2client!!
        if (!db2client.containsKey(db)) {
            val cxnString = "jdbc:mysql://$hostPort/${if (db != nodbTag) {
                db
            } else {
                ""
            }}?user=root"
            val pool = MySQLUtil.newMySQLConnectionPool(cxnString, 1)
            db2client[db] = MySQLUtil.Sql(pool)
        }
        return f(db2client[db]!!)
    }

    fun <T> q(f: (MySQLUtil.Sql) -> T): T {
        return q(nodbTag, f)
    }
}