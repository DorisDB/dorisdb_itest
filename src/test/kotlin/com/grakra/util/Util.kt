package com.grakra.util

import com.github.rholder.retry.BlockStrategies
import com.github.rholder.retry.RetryerBuilder
import com.github.rholder.retry.StopStrategies
import com.github.rholder.retry.WaitStrategies
import com.google.common.base.Strings
import com.grakra.TestMethodCapture
import org.slf4j.LoggerFactory
import org.testng.Assert
import java.io.*
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.Socket
import java.nio.channels.FileLock
import java.nio.channels.ServerSocketChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.attribute.PosixFilePermissions
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.LockSupport
import kotlin.math.abs
import kotlin.reflect.KFunction1
import kotlin.reflect.jvm.javaMethod
import kotlin.streams.toList


/**
 * Created by grakra on 18-8-30.
 */
object Util {
  private val LOG = LoggerFactory.getLogger(Util.javaClass)

  fun createPropertiesConfFile(path: String, props: List<Pair<String, String>>) {
    createFile(path) { o ->
      props.forEach { e ->
        o.println("${e.first}=${e.second}")
      }
    }
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
}