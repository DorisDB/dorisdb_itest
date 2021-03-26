package com.grakra.util

import org.springframework.jdbc.core.ColumnMapRowMapper
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.RowMapperResultSetExtractor
import org.springframework.jdbc.datasource.SimpleDriverDataSource
import org.springframework.jdbc.support.JdbcUtils
import java.sql.DriverManager
import java.sql.Statement
import org.apache.hive.jdbc.HiveConnection
import org.apache.hive.jdbc.HiveDriver
import java.util.concurrent.TimeUnit

object HiveUtil {
    fun getDataSource(driver: HiveDriver, cxnString: String): SimpleDriverDataSource {
        return SimpleDriverDataSource(driver, cxnString)
    }

    class Sql(val pool: ConnectionPool<HiveConnection>) {
        fun q(sql: String) = qq(pool, sql)
        fun qv(sql: String) {
            println("${Util.squeezeWhiteSpaces(sql)};")
            q(sql)!!.let { result ->
                result!!.forEach { rows ->
                    rows.entries.joinToString { (k, v) -> "$k=$v" }.let { println(it) }
                }
            }
        }

        fun e(sql: String) = e(sql, false)
        fun e(sql: String, dryRun:Boolean) :Boolean? {
            println("$sql;")
            if (dryRun){
                return true;
            }
            return ee(pool, sql)
        }
        fun close() = pool.queue.forEach { cxn -> cxn.close() }
    }

    fun newHiveConnectionPool(cxnString: String, user:String, password:String, size: Int): ConnectionPool<HiveConnection> {
        val init = {
            Class.forName(HiveDriver::class.java.canonicalName)
            Unit
        }
        val newCxn = {
            println(cxnString)
            DriverManager.getConnection(cxnString, user, password) as HiveConnection
        }
        return ConnectionPool(init, newCxn, size)
    }

    fun q(pool: ConnectionPool<HiveConnection>, sql: String): List<Map<String, Any>>? {
        val f = { stmt: Statement, sql: String ->
            val rsExtractor = RowMapperResultSetExtractor(ColumnMapRowMapper())
            rsExtractor.extractData(stmt.executeQuery(sql))
        }
        return x(pool, sql, f)
    }

    fun e(pool: ConnectionPool<HiveConnection>, sql: String): Boolean? {
        val f = { stmt: Statement, sql: String ->
            stmt.execute(sql)
        }
        return x(pool, sql, f)
    }

    fun qq(pool: ConnectionPool<HiveConnection>, sql: String): List<Map<String, Any>>? {
        val f = { stmt: Statement, sql: String ->
            val rsExtractor = RowMapperResultSetExtractor(ColumnMapRowMapper())
            rsExtractor.extractData(stmt.executeQuery(sql))
        }
        return xx(pool, sql, f)
    }

    fun ee(pool: ConnectionPool<HiveConnection>, sql: String): Boolean? {
        val f = { stmt: Statement, sql: String ->
            stmt.execute(sql)
            true
        }
        return xx(pool, sql, f)
    }

    fun <T> xx(pool: ConnectionPool<HiveConnection>, sql: String, f: (Statement, String) -> T): T? {
        return Util.retry {
            val houseKeeper = HouseKeeper()
            pool.exec { cxn ->
                val stmt = cxn.createStatement()
                Result.b.wrap {
                    houseKeeper.async {
                        f(stmt, sql)
                    }.get(Long.MAX_VALUE, TimeUnit.SECONDS)
                }.onErr {
                    it.printStackTrace()
                    Result.wrap { stmt.cancel() }
                }.onAny {
                    Result.wrap { JdbcUtils.closeStatement(stmt) }
                    Result.wrap { houseKeeper.shutdown() }
                }.unwrap()
            }.onErr {
                it.printStackTrace()
            }.unwrap_or_null()
        }
    }

    fun <T> x(pool: ConnectionPool<HiveConnection>, sql: String, f: (Statement, String) -> T): T? {
        return Util.retry {
            val houseKeeper = HouseKeeper()
            pool.exec_once { cxn ->
                val stmt = cxn.createStatement()
                Result.b.wrap {
                    houseKeeper.async {
                        f(stmt, sql)
                    }.get(Long.MAX_VALUE, TimeUnit.SECONDS)
                }.onErr {
                    it.printStackTrace()
                    Result.wrap { stmt.cancel() }
                }.onAny {
                    Result.wrap { JdbcUtils.closeStatement(stmt) }
                    Result.wrap { houseKeeper.shutdown() }
                }.unwrap()
            }.onErr {
                it.printStackTrace()
            }.unwrap_or_null()
        }
    }


    fun <T> runSql(timeout: Int, cxnString: String, f: (JdbcTemplate) -> T): T {
        return Util.retry {
            val driver = Class.forName(HiveDriver::class.java.canonicalName).newInstance() as HiveDriver
            val dataSource = getDataSource(driver, cxnString)
            val jdbc = JdbcTemplate(dataSource)
            jdbc.queryTimeout = timeout
            Result.b wrap {
                f(jdbc)
            } onAny {
                Result.wrap { dataSource.connection.close() }
            } unwrap Result.e
        }
    }
}
