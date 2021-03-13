package com.grakra.schema

import org.testng.annotations.Test

class TestSchema {
    @Test
    fun test() {
        OrcUtil.createOrcFile(
                "decimal_all_types.orc",
                FunctionTestTable.keyFields,
                FunctionTestTable.valueFields,
                40971,
                4096)
        //OrcUtil.readOrcFile("file0.orc")
    }


}
