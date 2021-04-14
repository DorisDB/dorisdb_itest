package com.grakra.tools

import com.grakra.schema.OrcUtil
import kotlin.system.exitProcess

fun main(vararg args:String) {
    if (args.size < 4){
        System.err.println("Format: CatOrc <file> <comma-separated-fields> <start_row> <num_rows>")
        exitProcess(1)
    }
    val file = args[0]
    val fieldNames = if (args[1] == "-"){
        listOf()
    } else {
        args[1].split(",")
    }

    val startRow = args[2].toInt()
    var numRows = args[3].toInt()
    OrcUtil.orcToCVS(file, startRow, numRows, *fieldNames.toTypedArray())
}