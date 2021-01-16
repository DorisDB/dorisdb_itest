package com.grakra

import com.grakra.util.Util
import org.testng.annotations.Test

class TestMisc {
    @Test
    fun testWriteOrcFile() {
        Util.createOrcFile("foobar.orc")
    }

    @Test
    fun testReadOrcFile(){
        Util.readOrcFile("foobar.orc")
    }
}