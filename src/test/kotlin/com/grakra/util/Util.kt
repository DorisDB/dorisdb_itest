package com.grakra.util

import com.github.rholder.retry.BlockStrategies
import com.github.rholder.retry.RetryerBuilder
import com.github.rholder.retry.StopStrategies
import com.github.rholder.retry.WaitStrategies
import com.google.common.base.Strings
import com.grakra.TestMethodCapture
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.common.type.HiveDecimal
import org.apache.hadoop.hive.ql.exec.vector.*
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable
import org.apache.orc.OrcFile
import org.apache.orc.TypeDescription
import org.slf4j.LoggerFactory
import org.stringtemplate.v4.STGroupString
import org.testng.Assert
import java.io.*
import java.math.BigDecimal
import java.math.BigInteger
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.Socket
import java.nio.channels.FileLock
import java.nio.channels.ServerSocketChannel
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.attribute.PosixFilePermissions
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.LockSupport
import java.util.stream.Collectors
import kotlin.math.abs
import kotlin.streams.toList


/**
 * Created by grakra on 18-8-30.
 */
object Util {
    private val LOG = LoggerFactory.getLogger(Util.javaClass)

    fun getTableSql(table: String): String {
        return this.javaClass.classLoader.getResourceAsStream("$table.sql").bufferedReader().readText()
    }

    fun squeezeWhiteSpaces(s: String): String {
        return s.replace(Regex("[\n\r]"), " ").replace(Regex("\\s+"), " ").trim()
    }

    fun createPropertiesConfFile(path: String, props: List<Pair<String, String>>) {
        createFile(path) { o ->
            props.forEach { e ->
                o.println("${e.first}=${e.second}")
            }
        }
    }

    fun generateRandomBigInt(negRatio: Int): () -> BigInteger {
        val rand = Random()
        return {
            if (rand.nextInt(100) < negRatio) {
                BigInteger(128, rand).negate()
            } else {
                BigInteger(128, rand)
            }
        }
    }

    fun generateRandomDecimal128(precision: Int, scale: Int, negRatio: Int): () -> BigDecimal {
        val maxValue = BigInteger(Strings.repeat("9", precision))
        val randomBigInt = generateRandomBigInt(negRatio)
        return {
            val bigInt = randomBigInt()
            if (bigInt.signum() < 0) {
                BigDecimal(bigInt.mod(maxValue), scale).negate()
            } else {
                BigDecimal(bigInt.mod(maxValue), scale)
            }
        }
    }

    fun generateRandomLong(numBits: Int): () -> Long {
        val rand = Random()
        val mask = (1L shl numBits) - 1L

        return {
            rand.nextLong().and(mask)
        }
    }

    fun generateRandomDecimal64(precision: Int, scale: Int): () -> Long {
        val maxValue = Strings.repeat("9", precision).toLong()
        val randomLong = generateRandomLong(64)
        return {
            randomLong() % maxValue
        }
    }

    fun createOrcFile(path: String) {
        val conf = Configuration()
        val dfsPath = org.apache.hadoop.fs.Path(path)
        val file = File(path)
        val schema = TypeDescription.createStruct()
                .addField("id", TypeDescription.createLong())
                .addField("decimal32", TypeDescription.createDecimal().withScale(3).withPrecision(9))
                .addField("decimal64", TypeDescription.createDecimal().withScale(6).withPrecision(18))
                .addField("decimal128", TypeDescription.createDecimal().withScale(9).withPrecision(38))
                .addField("nullable_decimal32", TypeDescription.createDecimal().withScale(3).withPrecision(9))
                .addField("nullable_decimal64", TypeDescription.createDecimal().withScale(6).withPrecision(18))
                .addField("nullable_decimal128", TypeDescription.createDecimal().withScale(9).withPrecision(38))

        if (Files.exists(file.toPath())) {
            file.delete()
        }
        val writer = OrcFile.createWriter(dfsPath, OrcFile.writerOptions(conf).setSchema(schema))
        val rowBatch = schema.createRowBatch()
        val idColumn = rowBatch.cols[0] as LongColumnVector
        val decimal32Column = rowBatch.cols[1] as DecimalColumnVector
        val decimal64Column = rowBatch.cols[2] as DecimalColumnVector
        val decimal128Column = rowBatch.cols[3] as DecimalColumnVector
        val nullableDecimal32Column = rowBatch.cols[4] as DecimalColumnVector
        val nullableDecimal64Column = rowBatch.cols[5] as DecimalColumnVector
        val nullableDecimal128Column = rowBatch.cols[6] as DecimalColumnVector
        val decimal32Gen = generateRandomDecimal128(9, 3, 25)
        val decimal64Gen = generateRandomDecimal128(18, 6, 25)
        val decimal128Gen = generateRandomDecimal128(38, 9, 25)
        val nullableDecimal32Gen = generateRandomDecimal128(9, 3, 25)
        val nullableDecimal64Gen = generateRandomDecimal128(18, 6, 25)
        val nullableDecimal128Gen = generateRandomDecimal128(38, 9, 25)
        for (r: Int in 0..4097) {
            val i = r % rowBatch.maxSize
            rowBatch.size++
            idColumn.vector[i] = r.toLong()
            decimal32Column.vector[i] = HiveDecimalWritable(HiveDecimal.create(decimal32Gen()))
            decimal64Column.vector[i] = HiveDecimalWritable(HiveDecimal.create(decimal64Gen()))
            decimal128Column.vector[i] = HiveDecimalWritable(HiveDecimal.create(decimal128Gen()))
            nullableDecimal32Column.vector[i] = HiveDecimalWritable(HiveDecimal.create(nullableDecimal32Gen()))
            nullableDecimal64Column.vector[i] = HiveDecimalWritable(HiveDecimal.create(nullableDecimal64Gen()))
            nullableDecimal128Column.vector[i] = HiveDecimalWritable(HiveDecimal.create(nullableDecimal128Gen()))
            if (rowBatch.size == rowBatch.maxSize) {
                writer.addRowBatch(rowBatch)
                rowBatch.reset()
            }
        }
        if (rowBatch.size != 0) {
            writer.addRowBatch(rowBatch)
            rowBatch.reset()
        }
        writer.close()
    }

    fun generateCounter(limit: Int): () -> Int {
        var n = -1
        return {
            n += 1
            n %= limit
            n
        }
    }

    fun generateCounter(): () -> Int = generateCounter(Int.MAX_VALUE)

    fun generateCounterRange(from: Int, till: Int): () -> Int {
        val counter = generateCounter(till - from)
        return { counter() + from }
    }

    fun generateCounterFrom(from: Int) = generateCounterRange(from, Int.MAX_VALUE)

    fun suffixCounter(prefix: String, counter: () -> Int): () -> String {
        return {
            "$prefix${counter()}"
        }
    }

    fun prefixCounter(suffix: String, counter: () -> Int): () -> String {
        return {
            "${counter()}${suffix}"
        }
    }

    fun nthItemOfColumn(vector: ColumnVector, desc: TypeDescription): (i: Int) -> String {
        return { i ->
            when (desc.category) {
                TypeDescription.Category.STRING,
                TypeDescription.Category.VARCHAR,
                TypeDescription.Category.CHAR,
                TypeDescription.Category.BINARY -> {
                    val binaryVector = vector as BytesColumnVector
                    String(binaryVector.vector[i], Charsets.UTF_8).take(10) + "..."
                }
                TypeDescription.Category.BOOLEAN,
                TypeDescription.Category.BYTE,
                TypeDescription.Category.SHORT,
                TypeDescription.Category.INT,
                TypeDescription.Category.LONG -> {
                    val byteVector = vector as LongColumnVector
                    byteVector.vector[i].toString()
                }
                TypeDescription.Category.FLOAT,
                TypeDescription.Category.DOUBLE -> {
                    val byteVector = vector as DoubleColumnVector
                    byteVector.vector[i].toString()
                }
                TypeDescription.Category.DATE -> {
                    val dateVector = vector as LongColumnVector
                    dateVector.vector[i].toString()
                }
                TypeDescription.Category.TIMESTAMP -> {
                    val datetimeVector = vector as TimestampColumnVector
                    datetimeVector.getTime(i).toString()
                }
                TypeDescription.Category.DECIMAL -> {
                    val decimalVector = vector as DecimalColumnVector
                    decimalVector.vector[i].hiveDecimal.toFormatString(desc.scale)
                }
                TypeDescription.Category.LIST -> TODO()
                TypeDescription.Category.MAP -> TODO()
                TypeDescription.Category.STRUCT -> TODO()
                TypeDescription.Category.UNION -> TODO()
            }
        }
    }

    fun readOrcFile(path: String) {
        val conf = Configuration()
        val dfsPath = org.apache.hadoop.fs.Path(path)
        val reader = OrcFile.createReader(dfsPath, OrcFile.ReaderOptions(conf))
        println(reader.schema)
        val rowBatch = reader.schema.createRowBatch()
        val columnFormatterGenerators = Array(reader.schema.fieldNames.size) {
            { vector: ColumnVector, desc: TypeDescription -> nthItemOfColumn(vector, desc) }
        }
        val columnFormatters = Array(reader.schema.fieldNames.size) {
            { _: Int -> "" }
        }
        val rows = reader.rows()
        while (rows.nextBatch(rowBatch)) {
            for (c: Int in 0 until rowBatch.numCols) {
                columnFormatters[c] = columnFormatterGenerators[c](rowBatch.cols[c], reader.schema.findSubtype(c + 1))
            }
            for (i: Int in 0 until rowBatch.size) {
                for (c: Int in 0 until rowBatch.numCols) {
                    print("${columnFormatters[c](i.toInt())},\t")
                }
                println()
            }
        }
        rows.close()
    }

    fun padLeading(s: String, c: Char, n: Int) =
            (Strings.repeat("$c", n) + s).takeLast(n)

    fun createPropertiesConfFile(path: String, props: Map<String, String>) {
        createPropertiesConfFile(path, props.entries.map { e -> e.key to e.value })
    }

    private fun createFile(path: String, writeCb: (PrintWriter) -> Unit) {
        Assert.assertFalse(path.isEmpty())
        val file = File(path)
        Assert.assertTrue(file.parentFile.exists())
        Assert.assertFalse(file.exists())
        val writer = PrintWriter(
                OutputStreamWriter(
                        FileOutputStream(file),
                        Charsets.UTF_8))
        writeCb(writer)
        writer.close()
    }

    private val fileLockDir = Paths.get("/tmp/dorisdb-port-locks")
    private val fileLocks = mutableMapOf<Path, FileLock>()

    init {
        Runtime.getRuntime().addShutdownHook(Thread {
            System.out.println("clean all file lock")
            fileLocks.forEach { f, fl ->
                fl.release()
                f.toFile().delete()
            }
        })
    }

    private fun createDirIfMissing(dir: Path, perm: String) {
        if (Files.notExists(dir)) {
            Files.createDirectories(dir)
            Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString(perm))
        }
    }

    fun classpath(vararg dirs: String): String {
        return dirs.map {
            val d = it.trimEnd('/')
            if (d.endsWith("conf") || d.endsWith("classes")) {
                d
            } else if (!File(d).isDirectory) {
                emptyArray<String>()
            } else {
                val df = File(d)
                df.listFiles { _, name ->
                    name.endsWith("jar")
                }.joinToString(":") { e ->
                    e.canonicalPath
                }
            }
        }.joinToString(":")
    }

    fun enclosedWriter(file: File, cb: (Writer) -> Unit) {
        file.outputStream().let {
            OutputStreamWriter(it, Charsets.UTF_8)
        }.let {
            cb(it)
            it.close()
        }
    }
    fun enclosedOutputStream(file: File, cb: (PrintStream) -> Unit) {
        file.outputStream().let {
            PrintStream(it)
        }.let {
            cb(it)
            it.flush()
            it.close()
        }
    }

    fun createFile(file: File, content: String) {
        Assert.assertTrue(!file.exists() || file.isFile)
        val f = file.outputStream()
        f.channel.truncate(0)
        f.write(content.toByteArray(Charsets.UTF_8))
        f.close()
    }

    fun <T> measureCost(tag: String, cb: () -> T): T {
        val begin = System.currentTimeMillis()
        try {
            return cb()
        } finally {
            val end = System.currentTimeMillis()
            println("[measureCost] $tag: ${(end - begin) / 1000}")
        }
    }

    fun streamCopy(ins: InputStream, outs: OutputStream) {
        val reader = BufferedReader(InputStreamReader(ins, Charsets.UTF_8))
        val printer = PrintWriter(OutputStreamWriter(outs, Charsets.UTF_8))
        while (true) {
            reader.readLine()?.let {
                printer.println(it)
                printer.flush()
            } ?: break
        }
    }

    fun readFile(file: File): List<String> {
        Assert.assertTrue(file.isFile)
        val reader = BufferedReader(
                InputStreamReader(file.inputStream(), Charsets.UTF_8))
        val lines = reader.lines().toList()
        reader.close()
        return lines
    }

    fun createLog4jPropertiesFile(file: File, logFile: String) {
        val log4jProperties = listOf(
                "log4j.rootLogger" to "INFO, console, FILE",
                "log4j.appender.console" to "org.apache.log4j.ConsoleAppender",
                "log4j.appender.console.layout" to "org.apache.log4j.PatternLayout",
                "log4j.appender.console.layout.ConversionPattern" to "%d [%-5p] %C{1}:%L %m%n",
                "log4j.appender.console.Threshold" to "WARN",
                "log4j.appender.FILE" to "org.apache.log4j.RollingFileAppender",
                "log4j.appender.FILE.Append" to "true",
                "log4j.appender.FILE.File" to logFile,
                "log4j.appender.FILE.Threshold" to "DEBUG",
                "log4j.appender.FILE.layout" to "org.apache.log4j.PatternLayout",
                "log4j.appender.FILE.layout.ConversionPattern" to "%d [%-5p] %C{1}:%L %m%n",
                "log4j.appender.FILE.MaxFileSize" to "10MB"
        )
        Util.createPropertiesConfFile(file.canonicalPath, log4jProperties)
    }

    fun flattenArgs(args: Array<out String>): Array<String> {
        return args.flatMap { s ->
            s.trim().split(Regex("\\s+"))
        }.toTypedArray()
    }

    fun randLong(bound: Long): () -> Long {
        val r = Random()
        return {
            abs(r.nextLong() % bound)
        }
    }

    fun randInt(bound: Int): () -> Int {
        val r = Random()
        return {
            abs(r.nextInt(bound))
        }
    }

    fun isPortAlive(host: String, port: Int): Boolean {
        val boxed = Result.wrap {
            val sock = Socket()
            sock.connect(InetSocketAddress(host, port), 1000)
            sock.close()
        }
        return boxed.isOk(true)
    }

    fun isPortAlive(port: Int): Boolean {
        return isPortAlive("127.0.0.1", port)
    }

    fun isPortDead(host: String, port: Int) = !isPortAlive(host, port)
    fun isPortDead(port: Int) = isPortDead("127.0.0.1", port)

    fun ensure(times: Int, failMsg: String, cb: () -> Boolean) {
        ensure(cb, failMsg, times)
    }

    fun ensure(cb: () -> Boolean, failMsg: String, times: Int = 60) {
        val rand = Random()
        for (i in (0..times)) {
            Result.wrap { Thread.sleep(1500 + rand.nextLong() % 500) }
            val boxed = Result.wrap(cb)
            if (!boxed.isOk(true)) continue
            if (boxed.unwrap()) return
        }
        Assert.fail("$failMsg, after 60 times")
    }

    fun ensurePortAlive(host: String, port: Int, failMsg: String) {
        ensure({ isPortAlive(host, port) }, failMsg)
    }

    fun ensurePortDead(host: String, port: Int, failMsg: String) {
        ensure({ isPortDead(host, port) }, failMsg)
    }

    fun unwind(g: () -> Unit, f: () -> Unit = {}) = { g();f() }

    @Synchronized
    fun allocateFreePort(): Int {
        createDirIfMissing(fileLockDir, "rwxrwxrwx")
        val kMinPort = 15000
        val kMaxPort = 30000
        val rand = Random()
        for (i in 0..1000) {
            val randPort = kMinPort + Math.abs(rand.nextInt()) % (kMaxPort - kMinPort + 1)
            LOG.debug("Trying to bind to port $randPort")
            val addr = InetSocketAddress(InetAddress.getLocalHost(), randPort)
            val sock = ServerSocketChannel.open()
            if (!Result.wrap { sock.bind(addr) }.isOk()) {
                continue
            }
            val fileLockPath = Paths.get(fileLockDir.toString(), "$randPort.lck")
            val boxedFile = Result.wrap { FileOutputStream(fileLockPath.toFile()) }
            if (!boxedFile.isOk()) {
                continue
            }
            val file = boxedFile.unwrap()
            val boxedFlock = Result.wrap { file.channel.tryLock() }
            if (!boxedFlock.isOk()) {
                Files.deleteIfExists(fileLockPath)
                sock.close()
                continue
            }
            fileLocks[fileLockPath] = boxedFlock.unwrap()
            if (!Result.wrap { sock.close() }.isOk()) {
                continue
            }
            return randPort
        }
        Assert.fail("Could not find a free random port between $kMinPort and $kMaxPort")
        return -1
    }

    fun getClusterBaseDir(clazz: Class<*>): String {
        val cwd = System.getProperty("user.dir")
        return cwd + File.separator + clazz.simpleName + ".dir"
    }

    fun getTestName(obj: Any) =
            "${obj::class.java.simpleName}.${TestMethodCapture.getTestMethod().methodName}"

    fun recreateDir(path: String) {
        removeDir(path)
        Assert.assertTrue(File(path).mkdirs())
    }

    fun removeDir(path: String) {
        val dir = File(path)
        if (dir.exists()) {
            dir.deleteRecursively()
        }
    }

    fun getRandSize(base: Int, delta: Int): Int {
        Assert.assertTrue(base > 0)
        Assert.assertTrue(delta in 0..base)
        return base + Random().nextInt() % delta
    }

    fun getRandRanges(n: Int, l: Int): Array<Pair<Int, Int>> {
        Assert.assertTrue(n > 0)
        Assert.assertTrue(l > 0)
        return Array(n) {
            val a = Math.abs(Random().nextInt()) % l
            val b = Math.abs(Random().nextInt()) % l
            if (a < b) a to b else b to a
        }
    }

    fun dump(e: Throwable) {
        var err = e
        while (true) {
            err.printStackTrace()
            if (err.cause == null) {
                break
            } else {
                println("caused by: ")
                err = err.cause!!
            }
        }
    }


    fun <T> retry(f: () -> T): T {
        val retryer = RetryerBuilder.newBuilder<T>()
                .retryIfException()
                .withStopStrategy(StopStrategies.stopAfterAttempt(3))
                .withBlockStrategy(BlockStrategies.threadSleepStrategy())
                .withWaitStrategy(WaitStrategies.exponentialWait(2000, 10, TimeUnit.SECONDS))
                .build()
        return Result.wrap {
            retryer.call(f)
        }.onErr {
            dump(it)
        }.unwrap()
    }

    fun <T> roundRobin(elms: List<T>): () -> T {
        Assert.assertTrue(elms.isNotEmpty())
        var i = AtomicInteger(0);
        return {
            elms[abs(i.getAndIncrement() % elms.size)]
        }
    }

    fun timed(timeout: Long, interval: Long, unit: TimeUnit, f: () -> Boolean): Boolean {
        val startMs = System.currentTimeMillis()
        val deadLine = startMs + unit.toMillis(timeout)
        while (true) {
            if (f()) {
                return true
            }
            val nowMs = System.currentTimeMillis()
            if (nowMs > deadLine) {
                return false
            }
            println("Timed elapse=${(nowMs - startMs) / 1000}s, remain=${(deadLine - nowMs) / 1000}s")
            LockSupport.parkNanos(unit.toNanos(interval))
        }
        return false
    }

    fun getClassPathOfJavaClass(clz: Class<*>) =
            clz.protectionDomain.codeSource.location.path!!


    fun times(atLeast: Int, atMost: Int): IntRange {
        return 1..atLeast + Random().nextInt() % (atMost + 1 - atLeast)
    }

    fun getRandSegments(l: Int, n: Int): IntArray {
        if (l < 2) {
            return intArrayOf(l)
        }

        Assert.assertTrue(n in 1 until l)
        val cuts = arrayOf(
                0,
                *Array(n) {
                    Util.getRandSize(l / 2, l / 2)
                },
                l
        )

        cuts.sort()
        val segments = mutableListOf<Int>()
        var init = cuts[0]
        cuts.slice(1..cuts.lastIndex).forEach { c ->
            segments.add(c - init)
            init = c
        }

        return segments.toIntArray()
    }

    fun renderTemplate(
            template: String,
            mainTemplateName: String,
            parameters: Map<String, Any?>): String {
        val st = STGroupString(template).getInstanceOf(mainTemplateName)
        val requiredKeys = st.attributes.keys
        val actualKeys = parameters.keys
        if (!actualKeys.containsAll(requiredKeys)) {
            requiredKeys.removeAll(actualKeys)
            val missingKeys = requiredKeys.stream().map { k: String -> "'$k'" }.collect(Collectors.joining(", "))
            throw Exception("Missing keys: $missingKeys")
        }
        parameters.forEach { (k: String, v: Any?) -> st.add(k, v) }
        return st.render()
    }

    fun renderTemplate(templateName: String, vararg parameters: Pair<String, Any?>): String {
        val template = String(this.javaClass.classLoader.getResourceAsStream(templateName).readAllBytes()!!, StandardCharsets.UTF_8)
        return renderTemplate(template, "main", parameters.toMap())
    }

    fun listResource(subdir: String, filter: (File)->Boolean):List<File>{
        val p = this.javaClass.classLoader.getResource(subdir).path
        return File(p).listFiles()!!.filter(filter)
    }
    fun listResource(subdir: String, fileExt:String):List<File> {
        return listResource(subdir){file->
            file.isFile && file.name.endsWith(fileExt)
        }
    }
}