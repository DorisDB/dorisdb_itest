package com.grakra.itest

import com.google.common.base.Strings
import com.grakra.dorisdb.DorisDBCluster
import com.grakra.util.Result
import com.grakra.util.Util
import org.slf4j.LoggerFactory
import org.testng.Assert
import org.testng.annotations.AfterClass
import org.testng.annotations.AfterMethod
import org.testng.annotations.BeforeClass
import org.testng.annotations.BeforeMethod
import java.io.File
import java.util.concurrent.ThreadLocalRandom

open class KotlinITest {
  val DORISDB_DOCKER_DIR = "/home/grakra/workspace/doris_docker"
  protected val rand = ThreadLocalRandom.current()
  protected val baseDir = File(Util.getClusterBaseDir(this.javaClass))
  var failed = false;

  companion object {
    val LOG = LoggerFactory.getLogger(KotlinITest::class.java)
  }

  @BeforeClass(alwaysRun = true)
  fun beforeClass() {
    if (baseDir.exists()) {
      Assert.assertTrue(baseDir.deleteRecursively())
    }
    Assert.assertTrue(baseDir.mkdirs())
    Result.wrap {
    }.onErr {
      failed = true
    }.unwrap()
  }

  @AfterClass(alwaysRun = true)
  fun afterClass() {
    if (!failed) {
      Assert.assertTrue(baseDir.deleteRecursively())
    }
  }

  @BeforeMethod(alwaysRun = true)
  fun beforeMethod() {
  }

  @AfterMethod(alwaysRun = true)
  fun afterMethod() {
  }

  fun do_test(f: (DorisDBCluster) -> Unit) {
    val dir = Util.getTestName(this)
    println("## TEST $dir: Enter ##")
    Util.recreateDir(dir)
    val cluster = DorisDBCluster(DORISDB_DOCKER_DIR, dir)
    Result.wrap {
      f(cluster)
    }.onAny {
      println("## TEST $dir: LEAVE ##")
    }.onOk {
      Util.removeDir(dir)
    }.unwrap()
  }

  protected fun enclosed(f: () -> Unit) {
    val line = Strings.repeat("=", 32)
    val testName = Util.getTestName(this)
    Result.b wrap {
      println(line)
      println("$testName RUN")
      println(line)
    } bind {
      f()
    } onErr {
      failed = true
      println(line)
      println("$testName FAIL")
      println(line)
    } onOk {
      failed = false
      println(line)
      println("$testName PASS")
      println(line)
    } unwrap Result.e
  }

  protected fun testHelp(
      doBefore: () -> Unit,
      injectFault: () -> Unit,
      doAfter: () -> Unit,
      verify: () -> Unit) {

    enclosed {
      Result.wrap {
        doBefore()
        injectFault()
        doAfter()
        verify()
      }.onErr {
        it.printStackTrace()
        failed = true
      }.unwrap()
    }
  }
}
