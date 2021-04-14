package com.grakra.itest

import com.grakra.TestMethodCapture
import com.grakra.schema.FixedLengthType
import com.grakra.schema.OrcUtil
import com.grakra.schema.SimpleField
import com.grakra.schema.Table
import com.grakra.util.Util
import org.testng.annotations.Listeners
import org.testng.annotations.Test

@Listeners(TestMethodCapture::class)
class OrcLoadTest : DorisDBRemoteITest() {

    @Test
    fun loadStringAsDecimal(){
        val db = "orc_test_db";
        val table = Table("orc_table0",
                listOf(
                        SimpleField.fixedLength("col_int", FixedLengthType.TYPE_INT),
                        SimpleField.decimalv2("col_decimalv2p20s2", 20, 2)
                ), 1)

        val table1= Table("orc_table0",
                listOf(
                        SimpleField.fixedLength("col_int", FixedLengthType.TYPE_INT),
                        SimpleField.varchar("col_decimalv2p20s2", 20)
                ), 1)

        val gen = Util.roundRobin(listOf(
                "-5.909999999999997".toByteArray(),
                "-5.909999999999997".toByteArray()))

        (1..10).forEach { println(String(gen())) }
        OrcUtil.createOrcFile(
                "orc_table0.orc",
                table1.keyFields(),
                table1.valueFields(emptySet()),
                100,
                4096
                , "col_decimalv2p20s2" to gen)
        OrcUtil.readOrcFile("orc_table0.orc")
    }
}
