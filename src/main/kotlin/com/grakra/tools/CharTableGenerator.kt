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
    if (args.size < 4) {
        println("Format: DecimalPerfTestDataGenerator <file-prefix> <num-files> <num-rows-per-file> <parallelism>")
        exitProcess(1)
    }

    val filePrefix = args[0]
    val numFiles = args[1].toInt()
    val numRowsPerFile = args[2].toInt()
    val parallelism = args[3].toInt()

    //val table = Tables.char_table
    val table = Tables.char_table
    val idGen = Util.generateLongCounter()
    var char255Gen = RandUtil.generateVarchar(255,255)
    var varchar65535Gen  = RandUtil.generateVarchar(65535, 65535)
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
            OrcUtil.createOrcFile(
                    fileName,
                    table.keyFields(),
                    table.valueFields(emptySet()),
                    numRowsPerFile,
                    4096,
                    "id" to idGen,
                    "col_char255" to char255Gen,
                    "col_nullable_char255" to char255Gen,
                    "col_varchar65535" to varchar65535Gen,
                    "col_nullable_varchar65535" to varchar65535Gen)
        }.addListener {
            atomicInt.decrementAndGet()
        }
    }
    while (atomicInt.get() > 0) {
        LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(500))
    }
    houseKeeper.shutdown()
}