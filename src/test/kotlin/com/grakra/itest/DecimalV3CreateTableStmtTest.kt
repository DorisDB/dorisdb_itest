package com.grakra.itest

import com.grakra.TestMethodCapture
import com.grakra.schema.Table
import org.testng.annotations.Listeners
import org.testng.annotations.Test

@Listeners(TestMethodCapture::class)
class DecimalV3CreateTableStmtTest:DorisDBRemoteITest() {
    @Test
    fun generate(){
    }
}