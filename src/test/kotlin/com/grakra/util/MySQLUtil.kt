package com.grakra.util

import com.mysql.jdbc.Driver
import com.mysql.jdbc.MySQLConnection
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.SimpleDriverDataSource
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.SQLType
import java.sql.Statement
import javax.sql.DataSource

object MySQLUtil {
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

  fun getDataSource(driver: Driver, cxnString: String): DataSource {
    return SimpleDriverDataSource(driver, cxnString)
  }

  fun <T> runSql(cxnString: String, f: (JdbcTemplate) -> T): T {
    val driver = Class.forName(Driver::class.java.canonicalName).newInstance() as Driver
    val dataSource = getDataSource(driver, cxnString)
    return f(JdbcTemplate(dataSource))
  }

}
