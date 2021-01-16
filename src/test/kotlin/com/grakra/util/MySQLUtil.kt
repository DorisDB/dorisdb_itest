package com.grakra.util

import com.mysql.jdbc.Driver
import com.mysql.jdbc.MySQLConnection
import org.springframework.jdbc.core.ColumnMapRowMapper
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.RowMapperResultSetExtractor
import org.springframework.jdbc.datasource.SimpleDriverDataSource
import org.springframework.jdbc.support.JdbcUtils
import java.sql.DriverManager
import java.sql.Statement
import java.util.concurrent.TimeUnit
import javax.sql.DataSource

object MySQLUtil {
  fun getDataSource(driver: Driver, cxnString: String): SimpleDriverDataSource {
    return SimpleDriverDataSource(driver, cxnString)
  }

  class Sql(val pool: ConnectionPool<MySQLConnection>) {
    fun q(sql: String) = qq(pool, sql)
    fun e(sql: String) = ee(pool, sql)
    fun close() = pool.queue.forEach { cxn -> cxn.close() }
  }

  fun newMySQLConnectionPool(cxnString: String, size: Int): ConnectionPool<MySQLConnection> {
    val init = {
      Class.forName(Driver::class.java.canonicalName)
      Unit
    }
    val newCxn = {
      println(cxnString)
      DriverManager.getConnection(cxnString).unwrap(MySQLConnection::class.java)!!
    }
    return ConnectionPool(init, newCxn, size)
  }

  fun q(pool: ConnectionPool<MySQLConnection>, sql: String): List<Map<String, Any>>? {
    val f = { stmt: Statement, sql: String ->
      val rsExtractor = RowMapperResultSetExtractor(ColumnMapRowMapper())
      rsExtractor.extractData(stmt.executeQuery(sql))
    }
    return x(pool, sql, f)
  }

  fun e(pool: ConnectionPool<MySQLConnection>, sql: String): Boolean? {
    val f = { stmt: Statement, sql: String ->
      stmt.execute(sql)
    }
    return x(pool, sql, f)
  }

  fun qq(pool: ConnectionPool<MySQLConnection>, sql: String): List<Map<String, Any>>? {
    val f = { stmt: Statement, sql: String ->
      val rsExtractor = RowMapperResultSetExtractor(ColumnMapRowMapper())
      rsExtractor.extractData(stmt.executeQuery(sql))
    }
    return xx(pool, sql, f)
  }

  fun ee(pool: ConnectionPool<MySQLConnection>, sql: String): Boolean? {
    val f = { stmt: Statement, sql: String ->
      stmt.execute(sql)
    }
    return xx(pool, sql, f)
  }

  fun <T> xx(pool: ConnectionPool<MySQLConnection>, sql: String, f: (Statement, String) -> T): T? {
    return Util.retry {
      val houseKeeper = HouseKeeper()
      pool.exec { cxn ->
        cxn.connectTimeout = 10
        cxn.socketTimeout = 10
        val stmt = cxn.createStatement()
        Result.b.wrap {
          houseKeeper.async {
            f(stmt, sql)
          }.get(10, TimeUnit.SECONDS)
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

  fun <T> x(pool: ConnectionPool<MySQLConnection>, sql: String, f: (Statement, String) -> T): T? {
    return Util.retry {
      val houseKeeper = HouseKeeper()
      pool.exec_once { cxn ->
        cxn.connectTimeout = 10
        cxn.socketTimeout = 10
        val stmt = cxn.createStatement()
        Result.b.wrap {
          houseKeeper.async {
            f(stmt, sql)
          }.get(10, TimeUnit.SECONDS)
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
      val driver = Class.forName(Driver::class.java.canonicalName).newInstance() as Driver
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
