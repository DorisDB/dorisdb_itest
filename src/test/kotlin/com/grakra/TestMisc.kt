package com.grakra

import com.google.common.base.Strings
import com.grakra.util.Util
import org.junit.Assert
import org.testng.annotations.Test
import java.math.BigDecimal
import java.math.BigInteger

class TestMisc {
    @Test
    fun testWriteOrcFile() {
        Util.createOrcFile("foobar.orc")
    }

    @Test
    fun testReadOrcFile() {
        Util.readOrcFile("foobar.orc")
    }

    fun gen_decimal_pair(num: Int, lhsGen: () -> BigDecimal, rhsGen: () -> BigDecimal): Array<Array<BigDecimal>> {
        return Array(num) { arrayOf(lhsGen(), rhsGen()) }
    }

    fun invalid_op(a: BigDecimal, b: BigDecimal):BigDecimal {
        Assert.fail("invalid_op")
        return BigDecimal.ZERO
    }
    fun add(a: BigDecimal, b: BigDecimal) = a.add(b)
    fun sub(a: BigDecimal, b: BigDecimal) = a.subtract(b)
    fun mul(a: BigDecimal, b: BigDecimal) = a.multiply(b)
    fun make_div(bits: Int): (BigDecimal, BigDecimal) -> BigDecimal {
        return { a, b ->
            val aIsInteger = a.remainder(BigDecimal.ONE).unscaledValue() == BigInteger.ZERO
            val ten = BigInteger.valueOf(10L)
            val aInt = a.unscaledValue()
            val bInt = b.unscaledValue()
            val bIsZero = bInt == BigInteger.ZERO
            val scaleFactor = if (aIsInteger) {
                ten.pow(b.scale() * 2)
            } else {
                ten.pow(b.scale())
            }
            if (bIsZero) {
                if (a.scale() == 0) {
                    BigDecimal(a.unscaledValue(), b.scale())
                } else {
                    a;
                }
            } else {
                val dividend = two_complement(aInt.multiply(scaleFactor), bits)
                val resultScale = if (aIsInteger) {
                    b.scale()
                } else {
                    a.scale()
                }
                BigDecimal(dividend.divide(b.unscaledValue()), resultScale)
            }
        }
    }

    fun make_mod(bits: Int): (BigDecimal, BigDecimal) -> BigDecimal {
        val div = make_div(bits)
        return { a, b ->
            val aIsInteger = a.remainder(BigDecimal.ONE).unscaledValue() == BigInteger.ZERO
            val ten = BigInteger.valueOf(10L)
            val aInt = a.unscaledValue()
            val bInt = b.unscaledValue()
            val bIsZero = bInt == BigInteger.ZERO
            val scaleFactor = if (aIsInteger) {
                ten.pow(b.scale() * 2)
            } else {
                ten.pow(b.scale())
            }
            if (bIsZero) {
                BigDecimal(BigInteger.ZERO, b.scale())
            } else {
                val dividend = two_complement(aInt.multiply(scaleFactor), bits)
                BigDecimal(dividend.remainder(b.unscaledValue()), b.scale())
            }
        }
    }

    fun decimal_pair2triple(
            pair_array: Array<Array<BigDecimal>>,
            f: (BigDecimal, BigDecimal) -> BigDecimal,
            g: (BigDecimal) -> BigDecimal?): Array<Array<BigDecimal?>> {
        return pair_array.map { (a, b) ->
            arrayOf(a, b, g(f(a, b)))
        }.toTypedArray()
    }

    fun decimal_triple2string(
            triples: Array<Array<BigDecimal?>>
    ): Array<Array<String>> {
        return triples.map { tr ->
            tr.map { e ->
                e?.let {
                    if (it.rem(BigDecimal.ONE).unscaledValue() == BigInteger.ZERO) {
                        it.toBigInteger().toString()
                    } else {
                        it.toPlainString()
                    }
                } ?: "null"
            }.toTypedArray()
        }.toTypedArray()
    }

    enum class OverflowPolicy {
        BINARY_BOUND_QUIET,
        DECIMAL_BOUND_QUIET,
        BINARY_BOUND_EXCLAIM,
        DECIMAL_BOUND_EXCLAIM,
    }

    fun max_bin_integer(bits: Int) =
            BigInteger.ONE.shiftLeft(bits - 1).subtract(BigInteger.ONE)!!

    fun min_bin_integer(bits: Int) =
            max_bin_integer(bits).negate().subtract(BigInteger.ONE)!!

    fun max_bin_decimal(bits: Int, scale: Int) =
            BigDecimal(max_bin_integer(bits), scale)

    fun min_bin_decimal(bits: Int, scale: Int) =
            BigDecimal(min_bin_integer(bits), scale)

    fun max_dec_integer(precision: Int) = BigInteger(Strings.repeat("9", precision))

    fun min_dec_integer(precision: Int) = max_dec_integer(precision).negate()!!

    fun max_dec_decimal(precision: Int, scale: Int) =
            BigDecimal(max_dec_integer(precision), scale)

    fun min_dec_decimal(precision: Int, scale: Int) =
            max_dec_decimal(precision, scale).negate()

    fun two_complement(v: BigInteger, bits: Int): BigInteger {
        val power2 = BigInteger.ONE.shiftLeft(bits)!!
        val mask32bits = power2 - BigInteger.ONE
        val v0 = v.and(mask32bits)!!
        return if (v0.testBit(bits - 1)) {
            power2.subtract(v0).negate()
        } else {
            v0
        }
    }

    fun overflow_policy(nbits: Int, precision: Int, policy: OverflowPolicy): (BigDecimal) -> BigDecimal? {
        val binMax = max_bin_integer(nbits)
        val binMin = min_bin_integer(nbits)
        val decMax = max_dec_integer(precision)
        val decMin = min_dec_integer(precision)
        when (policy) {
            OverflowPolicy.BINARY_BOUND_QUIET -> {
                return { v ->
                    val dec = v.unscaledValue()!!
                    if (dec > binMax || dec < binMin) {
                        BigDecimal(two_complement(dec, nbits), v.scale())
                    } else {
                        v
                    }
                }
            }
            OverflowPolicy.DECIMAL_BOUND_QUIET -> {
                return { v ->
                    val dec = v.unscaledValue()!!
                    if (dec > decMax || dec < decMin) {
                        BigDecimal(two_complement(dec, nbits), v.scale())
                    } else {
                        v
                    }
                }
            }
            OverflowPolicy.BINARY_BOUND_EXCLAIM -> {
                return { v ->
                    val dec = v.unscaledValue()!!
                    if (dec > binMax || dec < binMin) {
                        null
                    } else {
                        v
                    }
                }
            }
            OverflowPolicy.DECIMAL_BOUND_EXCLAIM -> {
                return { v ->
                    val dec = v.unscaledValue()!!
                    if (dec > decMax || dec < decMin) {
                        null
                    } else {
                        v
                    }
                }
            }
        }
    }

    fun genSpecialDecimals(t: DecimalType): Array<BigDecimal> {
        return arrayOf(
                //max_bin_decimal(t.bits, t.scale),
                //min_bin_decimal(t.bits, t.scale),
                max_dec_decimal(t.precision, t.scale),
                min_dec_decimal(t.precision, t.scale),
                BigDecimal(BigInteger.ZERO, t.scale),
                BigDecimal(BigInteger.ONE, t.scale),
                BigDecimal(BigInteger.ONE, t.scale).negate()
        )
    }

    fun generateTestCases(lhsType: DecimalType, rhsType: DecimalType, resultType: DecimalType, casesNum: Int, negRate: Int, op: (BigDecimal, BigDecimal) -> BigDecimal, overflowPolicy: OverflowPolicy) {
        val randInputs = gen_decimal_pair(
                casesNum,
                Util.generateRandomDecimal128(lhsType.precision, lhsType.scale, negRate),
                Util.generateRandomDecimal128(rhsType.precision, rhsType.scale, negRate))

        val lhsSpecials = genSpecialDecimals(lhsType)
        val rhsSpecials = genSpecialDecimals(rhsType)

        val specialInputs = lhsSpecials.flatMap { lhs ->
            rhsSpecials.map { rhs ->
                arrayOf(lhs, rhs)
            }
        }.toTypedArray()

        val testCases = decimal_triple2string(
                decimal_pair2triple(specialInputs.plus(randInputs), op, overflow_policy(resultType.bits, resultType.precision, overflowPolicy)))
        val content = Util.renderTemplate("decimal_testcase.template", "test_cases" to testCases)
        println(content)
    }

    fun generateFullTestCase(opName: String, lhsType: DecimalType, rhsType: DecimalType, resultType: DecimalType) {
        val randInputs = gen_decimal_pair(
                20,
                Util.generateRandomDecimal128(lhsType.precision, lhsType.scale, 50),
                Util.generateRandomDecimal128(rhsType.precision, rhsType.scale, 50))

        val lhsSpecials = genSpecialDecimals(lhsType)
        val rhsSpecials = genSpecialDecimals(rhsType)

        val specialInputs = lhsSpecials.flatMap { lhs ->
            rhsSpecials.map { rhs ->
                arrayOf(lhs, rhs)
            }
        }.toTypedArray()

        val primitiveType = "TYPE_DECIMAL${resultType.bits}"
        Assert.assertTrue(primitiveType in setOf("TYPE_DECIMAL32", "TYPE_DECIMAL64", "TYPE_DECIMAL128"))
        Assert.assertTrue(opName in setOf("AddOp", "SubOp", "MulOp", "DivOp", "ModOp"))
        val precisionsAndScales = arrayOf(
                lhsType.precision, lhsType.scale,
                rhsType.precision, rhsType.scale,
                resultType.precision, resultType.scale)

        val op = when(opName){
            "AddOp"->::add
            "SubOp"->::sub
            "MulOp"->::mul
            "DivOp"->make_div(resultType.bits)
            "ModOp"->make_mod(resultType.bits)
            else->::invalid_op
        }

        val testCases = decimal_triple2string(
                decimal_pair2triple(specialInputs.plus(randInputs), op, overflow_policy(resultType.bits, resultType.precision, OverflowPolicy.BINARY_BOUND_QUIET)))
        val content = Util.renderTemplate("decimal_testcase_full.template",
                "test_cases" to testCases,
                "primitive_type" to primitiveType,
                "binary_op" to opName,
                "precisions_and_scales" to precisionsAndScales)
        println(content)
    }

    @Test
    fun testGenerateDecimalAdd_32_9_2() {
        generateTestCases(
                DecimalType(32, 9, 2),
                DecimalType(32, 9, 2),
                DecimalType(32, 9, 2),
                20,
                50,
                ::add,
                OverflowPolicy.BINARY_BOUND_QUIET)
    }

    @Test
    fun testGenerateDecimalAdd_64_17_9() {
        generateTestCases(
                DecimalType(64, 17, 9),
                DecimalType(64, 17, 9),
                DecimalType(64, 17, 9),
                20,
                50,
                ::add,
                OverflowPolicy.BINARY_BOUND_QUIET)
    }

    @Test
    fun testGenerateDecimalAdd_128_17_9() {
        generateTestCases(
                DecimalType(128, 38, 38),
                DecimalType(128, 38, 38),
                DecimalType(128, 38, 38),
                20,
                50,
                ::add,
                OverflowPolicy.BINARY_BOUND_QUIET)
    }

    @Test
    fun testGenerateDecimalAddAdjustLeft_32_9_2() {
        generateTestCases(
                DecimalType(32, 6, 2),
                DecimalType(32, 4, 3),
                DecimalType(32, 9, 3),
                20,
                50,
                ::add,
                OverflowPolicy.BINARY_BOUND_QUIET)
    }

    @Test
    fun testGenerateDecimalAddAdjustLeft_64() {
        generateFullTestCase("AddOp",
                DecimalType(64,9,2),
                DecimalType(64, 18, 9),
                DecimalType(64,18,9)
        )
    }

    @Test
    fun testGenerateDecimalAddAdjustLeft_128() {
        generateFullTestCase(
                "AddOp",
                DecimalType(128, 30, 20),
                DecimalType(128, 38, 28),
                DecimalType(128, 38, 28))
    }

    @Test
    fun testGenerateDecimalAddAdjustRight_32_9_2() {
        generateTestCases(
                DecimalType(32, 5, 5),
                DecimalType(32, 6, 2),
                DecimalType(32, 9, 5),
                20,
                50,
                ::add,
                OverflowPolicy.BINARY_BOUND_QUIET)
    }

    @Test
    fun testGenerateDecimalAddAdjustRight_64(){
        generateFullTestCase(
                "AddOp",
                DecimalType(64, 18, 11),
                DecimalType(64, 12, 5),
                DecimalType(64, 18, 11))
    }

    @Test
    fun testGenerateDecimalAddAdjustRight_128() {
        generateFullTestCase(
                "AddOp",
                DecimalType(128, 38, 29),
                DecimalType(128, 28, 23),
                DecimalType(128, 38, 29))
    }

    @Test
    fun testGenerateDecimalSubAdjustLeft_32() {
        generateTestCases(
                DecimalType(32, 4, 2),
                DecimalType(32, 6, 4),
                DecimalType(32, 9, 4),
                20,
                50,
                ::sub,
                OverflowPolicy.BINARY_BOUND_QUIET)
    }
    @Test
    fun testGenerateDecimalSubAdjustLeft_64() {
        generateFullTestCase(
                "SubOp",
                DecimalType(64, 11, 5),
                DecimalType(64, 18, 12),
                DecimalType(64, 18, 12))
    }
    @Test
    fun testGenerateDecimalSubAdjustLeft_128() {
        generateFullTestCase(
                "SubOp",
                DecimalType(128, 31, 6),
                DecimalType(128, 34, 8),
                DecimalType(128, 38, 8))
    }

    @Test
    fun testGenerateDecimalSubAdjustRight_32_9_2() {
        generateTestCases(
                DecimalType(32, 6, 5),
                DecimalType(32, 4, 2),
                DecimalType(32, 9, 5),
                20,
                50,
                ::sub,
                OverflowPolicy.BINARY_BOUND_QUIET)
    }
    @Test
    fun testGenerateDecimalSubAdjustRight_64() {
        generateFullTestCase(
                "SubOp",
                DecimalType(64, 18, 13),
                DecimalType(64, 11, 6),
                DecimalType(64, 18, 13))
    }
    @Test
    fun testGenerateDecimalSubAdjustRight_128() {
        generateFullTestCase(
                "SubOp",
                DecimalType(128, 34, 7),
                DecimalType(128, 29, 5),
                DecimalType(128, 38, 7))
    }

    @Test
    fun testGenerateDecimalSub_32_9_2() {
        generateTestCases(
                DecimalType(32, 9, 2),
                DecimalType(32, 9, 2),
                DecimalType(32, 9, 2),
                20,
                50,
                ::sub,
                OverflowPolicy.BINARY_BOUND_QUIET)
    }

    @Test
    fun testGenerateDecimalSub_64() {
        generateFullTestCase(
                "SubOp",
                DecimalType(64, 18, 15),
                DecimalType(64, 18, 15),
                DecimalType(64, 18, 15))
    }
    @Test
    fun testGenerateDecimalSub_128() {
        generateFullTestCase(
                "SubOp",
                DecimalType(128, 38, 37),
                DecimalType(128, 38, 37),
                DecimalType(128, 38, 37))
    }

    @Test
    fun testGenerateDecimalMul_32_9_2() {
        generateTestCases(
                DecimalType(32, 5, 2),
                DecimalType(32, 5, 1),
                DecimalType(32, 9, 3),
                20,
                50,
                ::mul,
                OverflowPolicy.BINARY_BOUND_QUIET)
    }

    @Test
    fun testGenerateDecimalMul_64() {
        generateFullTestCase(
                "MulOp",
                DecimalType(64, 16, 4),
                DecimalType(64, 4, 3),
                DecimalType(64, 18, 7))
    }
    @Test
    fun testGenerateDecimalMul_128() {
        generateFullTestCase(
                "MulOp",
                DecimalType(128, 14, 7),
                DecimalType(128, 15, 13),
                DecimalType(128, 38, 20))
    }

    @Test
    fun testGenerateDecimalDiv_9_2() {
        generateTestCases(
                DecimalType(32, 7, 2),
                DecimalType(32, 3, 2),
                DecimalType(32, 9, 2),
                20,
                50,
                make_div(32),
                OverflowPolicy.BINARY_BOUND_QUIET)
    }

    @Test
    fun testGenerateDecimalDiv_64() {
        generateFullTestCase(
                "DivOp",
                DecimalType(64, 12, 8),
                DecimalType(64, 9, 6),
                DecimalType(64, 18, 8))
    }
    @Test
    fun testGenerateDecimalDiv_128() {
        generateFullTestCase(
                "DivOp",
                DecimalType(128, 15, 11),
                DecimalType(128, 25, 22),
                DecimalType(128, 38, 11))
    }

    @Test
    fun testGenerateDecimalMod_32() {
        generateTestCases(
                DecimalType(32, 7, 2),
                DecimalType(32, 3, 2),
                DecimalType(32, 9, 2),
                20,
                50,
                make_mod(32),
                OverflowPolicy.BINARY_BOUND_QUIET)
    }

    @Test
    fun testGenerateDecimalMod_64() {
        generateFullTestCase(
                "ModOp",
                DecimalType(64, 11, 8),
                DecimalType(64, 9, 7),
                DecimalType(64, 18, 7))
    }
    @Test
    fun testGenerateDecimalMod_128() {
        generateFullTestCase(
                "ModOp",
                DecimalType(128, 14, 11),
                DecimalType(128, 25, 24),
                DecimalType(128, 38, 24))
    }

    @Test
    fun testGenerateIntDivDecimal_32() {
        generateTestCases(
                DecimalType(32, 7, 0),
                DecimalType(32, 3, 1),
                DecimalType(32, 9, 1),
                20,
                50,
                make_div(32),
                OverflowPolicy.BINARY_BOUND_QUIET)
    }

    @Test
    fun testGenerateDecimalIntegerDivDecimal_64() {
        generateFullTestCase(
                "DivOp",
                DecimalType(64, 13, 0),
                DecimalType(64, 9, 3),
                DecimalType(64, 18, 3))
    }
    @Test
    fun testGenerateDecimalIntegerDivDecimal_128() {
        generateFullTestCase(
                "DivOp",
                DecimalType(128, 15, 0),
                DecimalType(128, 25, 17),
                DecimalType(128, 38, 17))
    }

    @Test
    fun testGenerateIntModDecimal_32() {
        generateTestCases(
                DecimalType(32, 7, 0),
                DecimalType(32, 3, 1),
                DecimalType(32, 9, 1),
                20,
                50,
                make_mod(32),
                OverflowPolicy.BINARY_BOUND_QUIET)
    }

    @Test
    fun testGenerateDecimalIntModDecimal_64() {
        generateFullTestCase(
                "ModOp",
                DecimalType(64, 10, 0),
                DecimalType(64, 9, 5),
                DecimalType(64, 18, 5))
    }
    @Test
    fun testGenerateDecimalIntModDecimal_128() {
        generateFullTestCase(
                "ModOp",
                DecimalType(128, 19, 0),
                DecimalType(128, 25, 10),
                DecimalType(128, 38, 10))
    }

    @Test
    fun testGenerateDecimalDivInteger_32() {
        generateTestCases(
                DecimalType(32, 9, 3),
                DecimalType(32, 3, 0),
                DecimalType(32, 9, 3),
                20,
                50,
                make_div(32),
                OverflowPolicy.BINARY_BOUND_QUIET)
    }
    @Test
    fun testGenerateDecimalDivInteger_64() {
        generateFullTestCase(
                "DivOp",
                DecimalType(64, 17, 2),
                DecimalType(64, 15, 0),
                DecimalType(64, 18, 2))
    }
    @Test
    fun testGenerateDecimalDivInteger_128() {
        generateFullTestCase(
                "DivOp",
                DecimalType(128, 25, 17),
                DecimalType(128, 9, 0),
                DecimalType(128, 38, 17))
    }

    @Test
    fun testGenerateDecimalModInteger_32() {
        generateTestCases(
                DecimalType(32, 9, 3),
                DecimalType(32, 3, 0),
                DecimalType(32, 9, 0),
                20,
                50,
                make_mod(32),
                OverflowPolicy.BINARY_BOUND_QUIET)
    }
    @Test
    fun testGenerateDecimalModInteger_64() {
        generateFullTestCase(
                "ModOp",
                DecimalType(64, 17, 5),
                DecimalType(64, 11, 0),
                DecimalType(64, 18, 0))
    }
    @Test
    fun testGenerateDecimalModInteger_128() {
        generateFullTestCase(
                "ModOp",
                DecimalType(128, 37, 10),
                DecimalType(128, 19, 0),
                DecimalType(128, 38, 0))
    }

    @Test
    fun testDiv() {
        val div = make_div(32)
        val a = div(BigDecimal(BigInteger("99999"), 0), BigDecimal(BigInteger.ZERO, 2))
        val b = div(BigDecimal(BigInteger("99999"), 4), BigDecimal(BigInteger.ZERO, 2))
        println(a)
        println(b)
        // -59401, rhs=-1.21

        val c = div(BigDecimal(BigInteger("-59401"), 0), BigDecimal(BigInteger("-121"), 2))
        println(c)
    }

    @Test
    fun test() {
        val v = BigInteger.ONE.shiftLeft(31)
        println(two_complement(v, 32))
    }

    @Test
    fun test1() {
        val max_bin = max_bin_decimal(32, 0)
        val min_bin = min_bin_decimal(32, 0)
        val max_dec = max_dec_decimal(9, 0)
        val min_dec = min_dec_decimal(9, 0)
        println(max_bin)
        println(min_bin)
        println(max_dec)
        println(min_dec)
    }

    @Test
    fun test2() {
        val value = BigInteger("99999000")
        val dec = BigDecimal(value, 3)
        val a = dec.remainder(BigDecimal.ONE)
        println(a);
        println(a.unscaledValue() == BigInteger.ZERO)
    }
}