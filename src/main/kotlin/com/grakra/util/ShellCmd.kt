package com.grakra.util

import com.grakra.AbstractProcess
import org.testng.Assert
import java.io.File

/**
 * Created by grakra on 18-9-4.
 */
class ShellCmd(override val baseDir: String) : AbstractProcess() {
  override var cmd = ""
  override var argv = arrayOf<String>()
  override fun prepare() {
    cleanup()
    Result.wrap { File(baseDir).mkdirs() }.unwrap()
  }

  fun run_nocheck(vararg cmdAndArgs: String) {
    run_and_check(false, *cmdAndArgs)
  }

  fun run(vararg cmdAndArgs: String) {
    run_and_check(true, *cmdAndArgs)
  }

  fun run_and_check(check: Boolean, vararg cmdAndArgs: String) {
    val args = cmdAndArgs.flatMap { s ->
      s.trim().split(Regex("\\s+"))
    }.toTypedArray()
    this.cmd = args[0]
    this.argv = args.drop(1).toTypedArray()
    startAndWait(check)
  }

  fun run_script(name: String, script: String) {
    this.cmd = "/bin/bash"
    Assert.assertTrue(Regex("^\\w+$").matches(name))
    val scriptFile = File(baseDir + File.separator + name)
    Util.createFile(scriptFile, script)
    this.argv = arrayOf(scriptFile.canonicalPath)
    startAndWait(true)
  }
}