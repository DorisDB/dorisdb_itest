package com.grakra.itest

import com.grakra.TestMethodCapture
import com.grakra.util.Util
import org.junit.Assert
import org.testng.annotations.Listeners
import org.testng.annotations.Test

@Listeners(TestMethodCapture::class)
class DecimalTest : KotlinITest() {
  @Test
  fun testDecimal() {
    val randi = Util.randLong(1000000000000000000L)
    val randf = Util.randLong(1000000000L)
    val decimals = Array(5) {
      Util.padLeading("${randi()}", '0', 18) + "." + Util.padLeading("${randf()}", '0', 9)
      //"0." + Util.padLeading("${randf()}", '0', 9)
    }
    decimals.forEach(::println)
    do_test { c ->
      val j = c.getSqlClient(null)
      val f0 = { name: String ->
        decimals.map { d ->
          val s = "select cast($name(cast('$d' as decimal(27,9))) as double) = $name(cast(cast('$d' as decimal(27,9)) as double))"
          println("SQL:$s;")
          j.q(s)?.first()?.values?.first()?.let{b->Assert.assertTrue(b as Boolean)}?:Assert.fail("FAIL SQL:$s")
        }
      }
      listOf(
          "abs","sign", "sin","cos","tan",
          "ceil","floor","round","log10", "log2", "exp", "radians",
          "degrees", "sqrt"
      ).forEach { name ->
        f0(name)
      }
      j.close()
    }
  }
}
