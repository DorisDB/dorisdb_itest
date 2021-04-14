package com.grakra.util

class HiveClient(private val hostPort: String, private val user:String, private val password:String) {
    private var db2client: MutableMap<String, HiveUtil.Sql>? by thread_local()
    private val nodbTag = "!!0xdeadbeef!!"
    fun <T> q(db: String, f: (HiveUtil.Sql) -> T): T {
        if (db2client == null) {
            db2client = mutableMapOf()
        }
        val db2client = db2client!!
        if (!db2client.containsKey(db)) {
            val cxnString = "jdbc:hive2://$hostPort"
            val pool = HiveUtil.newHiveConnectionPool(cxnString, user, password, 1)
            db2client[db] = HiveUtil.Sql(pool)
        }
        return f(db2client[db]!!)
    }

    fun <T> q(f: (HiveUtil.Sql) -> T): T {
        return q(nodbTag, f)
    }
}