package com.grakra.tools

import com.grakra.schema.*
import com.grakra.tables.Tables
import com.grakra.util.HouseKeeper
import com.grakra.util.RandUtil
import com.grakra.util.Util
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.LockSupport
import kotlin.system.exitProcess

fun main(vararg args: String) {
    if (args.size < 6) {
        println("Format: DecimalPerfTestDataGenerator <file-prefix> <num-files> <num-rows-per-file> <nullable> <null-ratio> <parallelism>")
        exitProcess(1)
    }

    val filePrefix = args[0]
    val numFiles = args[1].toInt()
    val numRowsPerFile = args[2].toInt()
    val nullable = args[3] == "nullable"
    val nullRatio = args[4].toInt()
    val parallelism = args[5].toInt()

    val table = Tables.decimal_perf_table

    val tableNew = if (nullable) {
        Table(table.tableName, table.keyFields() + table.valueFields(emptySet()).map {
            CompoundField.nullable(it.simple(), nullRatio)
        }, table.keyLimit)
    } else {
        table
    }
    val idGen = Util.generateLongCounter()
    val decimalP7S2Gen = RandUtil.generateRandomDecimalBinary(7, 2)
    val decimalP15S6Gen = RandUtil.generateRandomDecimalBinary(15, 6)
    val houseKeeper = HouseKeeper()
    val atomicInt = AtomicInteger(0)
    (1..numFiles).forEach { n ->
        val suffix = "0000$n".takeLast(4)
        val fileName = "${filePrefix}_${suffix}.orc"
        while (atomicInt.get() >= parallelism) {
            LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(500))
        }
        atomicInt.incrementAndGet()
        houseKeeper.async {
            OrcUtil.createOrcFile(fileName, tableNew.keyFields(), tableNew.valueFields(emptySet()), numRowsPerFile, 65536,
                    "id" to idGen, "col_varchar" to decimalP7S2Gen, "col_char" to decimalP15S6Gen)
        }.addListener {
            atomicInt.decrementAndGet()
        }
    }
    while (atomicInt.get() > 0) {
        LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(500))
    }
    houseKeeper.shutdown()
}