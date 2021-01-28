package com.grakra.generators

import com.grakra.DecimalType
import com.grakra.schema.DecimalField
import com.grakra.schema.FixedLengthType
import com.grakra.schema.OrcUtil
import com.grakra.schema.SimpleField
import com.grakra.util.DecimalOverflowCheck
import com.grakra.util.Util
import org.testng.annotations.Test
import java.io.File
import java.math.BigDecimal
import java.math.BigInteger
import java.sql.Date
import java.sql.Timestamp

class GenerateDecimalCastTestCases {

    data class CastTestCase(
            val inputPrecision: Int,
            val inputScale: Int,
            val inputValue: String,
            val outputPrecision: Int,
            val outputScale: Int,
            val outputValue: String,
            val nullValue: Boolean
    )

    fun generateTest(
            tcList: List<CastTestCase>,
            fromType: String,
            toType: String,
            name: String
    ): String {
        return Util.renderTemplate("decimal_cast.template",
                "testCases" to tcList,
                "fromType" to fromType,
                "toType" to toType,
                "name" to name
        )
    }

    fun bigIntegerToDecimal(bigInteger: BigInteger, decimalType: DecimalType): BigDecimal? {
        val v = bigInteger.multiply(BigInteger.valueOf(10).pow(decimalType.scale))!!
        val checkOverflow = DecimalOverflowCheck.overflow_policy(
                decimalType.bits,
                decimalType.precision,
                DecimalOverflowCheck.OverflowPolicy.BINARY_BOUND_EXCLAIM)
        return checkOverflow(BigDecimal(v, decimalType.scale))
    }

    fun decimalToDecimal(d: BigDecimal, decimalType: DecimalType): BigDecimal? {
        val scale = d.scale()
        val checkOverflow = DecimalOverflowCheck.overflow_policy(
                decimalType.bits,
                decimalType.precision,
                DecimalOverflowCheck.OverflowPolicy.BINARY_BOUND_EXCLAIM)
        return if (scale <= decimalType.scale) {
            val adjustScaleFactor = BigInteger.valueOf(10L).pow(decimalType.scale - scale)!!
            val adjustDecimal = BigDecimal(d.unscaledValue().multiply(adjustScaleFactor), decimalType.scale)
            checkOverflow(adjustDecimal)
        } else {
            val adjustScaleFactor = BigInteger.valueOf(10L).pow(scale - decimalType.scale)!!
            val adjustDecimal = BigDecimal(d.unscaledValue().divide(adjustScaleFactor), decimalType.scale)
            checkOverflow(adjustDecimal)
        }
    }

    fun doubleToDecimal(d: Double, decimalType: DecimalType): BigDecimal? {
        val scaleFactor = BigInteger.valueOf(10L).pow(decimalType.scale)!!
        val decimal = (d.toBigDecimal() * scaleFactor.toBigDecimal())!!
        return decimalToDecimal(decimal, decimalType)
    }


    fun toDecimal(value: Any, decimalType: DecimalType): BigDecimal? {
        return when (value) {
            is Boolean -> {
                val v = BigInteger.valueOf(if (value == true) {
                    1L
                } else {
                    0L
                })!!
                bigIntegerToDecimal(v, decimalType)
            }
            is Byte, is Short, is Int, is Long, is BigInteger -> {
                val v = BigInteger(value.toString())
                bigIntegerToDecimal(v, decimalType)
            }
            is Date -> {
                val v = BigInteger.valueOf(value.time)!!
                bigIntegerToDecimal(v, decimalType)
            }
            is Timestamp -> {
                val v = BigInteger.valueOf(value.time)!!
                bigIntegerToDecimal(v, decimalType)
            }
            is Float -> {
                doubleToDecimal(value.toDouble(), decimalType)
            }
            is Double -> {
                doubleToDecimal(value, decimalType)
            }
            is BigDecimal -> {
                decimalToDecimal(value, decimalType)
            }
            else -> TODO()
        }
    }

    fun generateAllTest() {
        val name = Util.suffixCounter("col", Util.generateCounter())
        val fromFields = listOf<SimpleField>(
                SimpleField.fixedLength(name(), FixedLengthType.TYPE_BOOLEAN),
                SimpleField.fixedLength(name(), FixedLengthType.TYPE_TINYINT),
                SimpleField.fixedLength(name(), FixedLengthType.TYPE_SMALLINT),
                SimpleField.fixedLength(name(), FixedLengthType.TYPE_INT),
                SimpleField.fixedLength(name(), FixedLengthType.TYPE_BIGINT),
                SimpleField.fixedLength(name(), FixedLengthType.TYPE_LARGEINT),
                SimpleField.fixedLength(name(), FixedLengthType.TYPE_FLOAT),
                SimpleField.fixedLength(name(), FixedLengthType.TYPE_DOUBLE),
                SimpleField.fixedLength(name(), FixedLengthType.TYPE_DECIMALV2),
                SimpleField.fixedLength(name(), FixedLengthType.TYPE_DATE),
                SimpleField.fixedLength(name(), FixedLengthType.TYPE_DATETIME),

                SimpleField.decimal(name(), 32, 9, 0),
                SimpleField.decimal(name(), 32, 9, 2),
                SimpleField.decimal(name(), 32, 9, 9),
                SimpleField.decimal(name(), 32, 7, 4),

                SimpleField.decimal(name(), 64, 18, 0),
                SimpleField.decimal(name(), 64, 18, 2),
                SimpleField.decimal(name(), 64, 18, 9),
                SimpleField.decimal(name(), 64, 18, 18),
                SimpleField.decimal(name(), 64, 15, 13),

                SimpleField.decimal(name(), 128, 38, 0),
                SimpleField.decimal(name(), 128, 38, 2),
                SimpleField.decimal(name(), 128, 38, 9),
                SimpleField.decimal(name(), 128, 38, 13),
                SimpleField.decimal(name(), 128, 38, 18),
                SimpleField.decimal(name(), 128, 35, 30)
        )

        val toFields = listOf<DecimalField>(
                SimpleField.decimal(name(), 32, 9, 0),
                SimpleField.decimal(name(), 32, 9, 2),
                SimpleField.decimal(name(), 32, 9, 9),
                SimpleField.decimal(name(), 32, 7, 4),

                SimpleField.decimal(name(), 64, 18, 0),
                SimpleField.decimal(name(), 64, 18, 2),
                SimpleField.decimal(name(), 64, 18, 9),
                SimpleField.decimal(name(), 64, 18, 18),
                SimpleField.decimal(name(), 64, 15, 13),

                SimpleField.decimal(name(), 128, 38, 0),
                SimpleField.decimal(name(), 128, 38, 2),
                SimpleField.decimal(name(), 128, 38, 9),
                SimpleField.decimal(name(), 128, 38, 13),
                SimpleField.decimal(name(), 128, 38, 18),
                SimpleField.decimal(name(), 128, 35, 30)
        )
        val randGens = OrcUtil.getDefaultGenerators(fromFields)
        val cases = fromFields.flatMap { fa ->
            toFields.flatMap { fb ->
                val gen = randGens.getValue(fa.name)
                val fromType = fa.primitiveType()
                val toType = fb.primitiveType()
                val fromUniqueType = fa.uniqueType()
                val toUniqueType = fb.uniqueType();
                val (fromPrecision, fromScale) = fa.precisionAndScale()
                val (toPrecision, toScale) = fb.precisionAndScale()
                val name = "testCastOf${fromUniqueType}And${toUniqueType}"
                val testCases = Array(20) {
                    val value = gen()
                    val decimalValue = toDecimal(value, DecimalType(fb.bits, fb.precision, fb.scale))
                    val isNull = decimalValue?.let { false} ?:true
                    val outputValue = decimalValue?.let { decimalValue } ?: BigDecimal(BigInteger.ZERO, fb.scale)
                    val outputValueString = outputValue.stripTrailingZeros().toPlainString()
                    CastTestCase(fromPrecision, fromScale, "${value}", toPrecision, toScale, "$outputValueString", isNull)
                }
                val (failTestCases, passTestCases) = testCases.partition { it -> it.nullValue }
                listOf(
                        failTestCases.size to generateTest(failTestCases.toList(), fromType, toType, "${name}Abnormal"),
                        passTestCases.size to generateTest(passTestCases.toList(), fromType, toType, "${name}Normal")).filter {
                    it.first>0
                }.map { it.second }
            }
        }
        Util.createFile(File("decimal_cast_test.txt"),cases.joinToString("\n\n"))
    }

    @Test
    fun test() {
        val tcList = listOf(
                CastTestCase(-1, -1, "0.34", 9, 2, "0.34", false),
                CastTestCase(-1, -1, "0.35", 9, 2, "0.34", false)
        )
        println(generateTest(tcList, "TYPE_FLOAT", "TYPE_DECIMAL32", "testCastOfFloatAndDecimal32"))
    }

    @Test
    fun testAllUT() {
        generateAllTest()
    }
}
