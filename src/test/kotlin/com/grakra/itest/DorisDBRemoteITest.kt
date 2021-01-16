package com.grakra.itest

import com.google.common.base.Preconditions
import com.grakra.TestMethodCapture
import com.grakra.dorisdb.MySQLClient
import org.testng.annotations.Listeners


@Listeners(TestMethodCapture::class)
open class DorisDBRemoteITest : KotlinITest() {
    private val hostPort = System.getenv("DORISDB_FE_MASTER_HOSTPORT") ?: "39.103.134.93:8338"

    init {
        Preconditions.checkState(hostPort.matches(Regex("\\d+(\\.\\d+){3}:\\d+")))
    }

    private val mysql = MySQLClient(hostPort)
    fun <T> run_mysql(f: (MySQLClient) -> T): T {
        return f(mysql)
    }

}