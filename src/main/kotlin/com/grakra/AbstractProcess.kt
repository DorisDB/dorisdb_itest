package com.grakra

import com.grakra.util.Result
import com.grakra.util.Util
import org.testng.Assert
import java.io.*

abstract class AbstractProcess {
  abstract val cmd: String
  abstract val argv: Array<String>
  abstract val baseDir: String
  var proc: Process? = null
  var prepared = false

  val stdout = ByteArrayOutputStream()
  val stderr = ByteArrayOutputStream()
  val cwd = File(System.getProperty("user.dir")!!)
  val verbose = System.getenv("verbose")

  open val envs = mapOf<String, String>()
  protected abstract fun prepare()
  fun cleanup() {
    Result.wrap {
      val df = File(baseDir)
      df.deleteRecursively()
    }.unwrap()
  }

  fun start() {
    if (!prepared) {
      prepare()
      prepared = true
    }
    proc = Result.wrap {
      val cmdAndArgs = arrayOf(cmd, *argv)
      val cmdAndArgsStr = cmdAndArgs.joinToString(" ")
      var script = "#!/bin/bash\n$cmdAndArgsStr"
      if (envs.isNotEmpty()) {
        val envsStr = envs.map { e -> "${e.key}=${e.value}" }.joinToString(" ")
        script = "#!/bin/bash\nenv $envsStr $cmdAndArgsStr"
      }
      println("[RUN CMD]: $cmdAndArgsStr")
      verbose?.let {
        println("==========================")
        println("[RUN CMD]\n$script")
      }
      val scriptFile = File(baseDir + File.separator + "script.sh")
      Util.createFile(scriptFile, script)

      val builder = ProcessBuilder(*cmdAndArgs)
      builder.environment().putAll(envs)
      builder.start()
    }.unwrap()

    Assert.assertNotNull(proc)

    Result.wrap {
      Util.streamCopy(proc!!.inputStream, stdout)
      stdout.close()
    }
    Result.wrap {
      Util.streamCopy(proc!!.errorStream, stderr)
      stderr.close()
    }
  }

  fun startAndWait(check: Boolean) {
    this.start()
    if (check) {
      val status = waitFor()
      if (status != 0) {
        println("stderr:\n${this.stderr}")
        println("stdout:\n${this.stdout}")
        Assert.fail("Fail to exec ${argv.joinToString(" ")}")
      }
    }
  }

  fun kill() {
    val p = proc
    p ?: return
    if (p.isAlive) {
      p.destroy()
    }
  }

  fun killForcibly() {
    val p = proc
    p ?: return
    if (p.isAlive) {
      p.destroyForcibly()
    }
  }

  fun waitFor(): Int {
    val p = proc
    val exitCode = p?.let { p.waitFor() } ?: 0
    proc = null
    return exitCode
  }

  fun stop() {
    killForcibly()
    waitFor()
  }
}