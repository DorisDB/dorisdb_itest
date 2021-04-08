package com.grakra.itest

import com.google.common.base.Preconditions
import com.grakra.DecimalType
import com.grakra.TestMethodCapture
import com.grakra.schema.*
import com.grakra.util.DecimalUtil
import com.grakra.util.RandUtil
import com.grakra.util.Util
import net.bytebuddy.asm.AsmVisitorWrapper
import org.testng.Assert
import org.testng.annotations.AfterClass
import org.testng.annotations.BeforeClass
import org.testng.annotations.Listeners
import org.testng.annotations.Test
import java.io.File
import java.io.PrintStream
import java.math.BigDecimal
import java.math.BigInteger
import java.sql.Date
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.*


@Listeners(TestMethodCapture::class)
class DecimalCastTest : DorisDBRemoteITest() {

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
            name: String,
            category: String,
            fail: String
    ): String {
        return Util.renderTemplate("decimal_cast.template",
                "testCases" to tcList,
                "fromType" to fromType,
                "toType" to toType,
                "name" to name,
                "category" to category,
                "fail" to fail
        )
    }

    fun bigIntegerToDecimal(bigInteger: BigInteger, decimalType: DecimalType): BigDecimal? {
        val v = bigInteger.multiply(BigInteger.valueOf(10).pow(decimalType.scale))!!
        val checkOverflow = DecimalUtil.overflow_policy(
                decimalType.bits,
                decimalType.precision,
                DecimalUtil.OverflowPolicy.BINARY_BOUND_EXCLAIM)
        return checkOverflow(BigDecimal(v, decimalType.scale))
    }

    fun decimalToDecimal(d: BigDecimal, decimalType: DecimalType): BigDecimal? {
        val scale = d.scale()
        val checkOverflow = DecimalUtil.overflow_policy(
                decimalType.bits,
                decimalType.precision,
                DecimalUtil.OverflowPolicy.DECIMAL_BOUND_EXCLAIM)
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
        val checkOverflow = DecimalUtil.overflow_policy(
                decimalType.bits,
                decimalType.precision,
                DecimalUtil.OverflowPolicy.BINARY_BOUND_EXCLAIM)
        val scaleFactor = BigInteger.valueOf(10L).pow(decimalType.scale)!!
        val doubleValue = d * scaleFactor.toDouble()
        val bigIntValue = when (decimalType.bits) {
            32 -> doubleValue.toInt().toLong().toBigInteger()
            64 -> doubleValue.toLong().toBigInteger()
            128 -> BigDecimal.valueOf(doubleValue).toBigInteger()
            else -> {
                Preconditions.checkArgument(false)
                BigInteger.ZERO
            }
        }
        return checkOverflow(DecimalUtil.intPart(BigDecimal.valueOf(doubleValue)))?.let {
            BigDecimal(bigIntValue, decimalType.scale)
        }
    }

    fun dateToInt(date: Date): Int {
        val calendar = Calendar.getInstance()
        calendar.time = date
        val year = calendar.get(Calendar.YEAR).let { it ->
            when (it) {
                in (10000..Int.MAX_VALUE) -> it - 10000
                else -> it
            }
        }
        val month = calendar.get(Calendar.MONTH) + 1
        var day = calendar.get(Calendar.DAY_OF_MONTH)
        return year * 10000 + month * 100 + day
    }

    fun intToDate(value: Int): Date {
        return Date(java.util.Date().time)
    }

    fun longToTimestamp(value: Long): Timestamp {
        return Timestamp(Date().time)
    }

    fun timestampToLong(ts: Timestamp): Long {
        val calendar = Calendar.getInstance()
        val date = Date(ts.time)
        calendar.time = date
        var hour = calendar.get(Calendar.HOUR_OF_DAY)
        var minute = calendar.get(Calendar.MINUTE)
        var secend = calendar.get(Calendar.SECOND)
        return (dateToInt(date) * 1_000000L +
                hour * 1_0000L + minute * 100L + secend)
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
                bigIntegerToDecimal(dateToInt(value).toBigInteger(), decimalType)
            }
            is Timestamp -> {
                bigIntegerToDecimal(timestampToLong(value).toBigInteger(), decimalType)
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

    fun getIntegerFields(): List<SimpleField> {
        val name = Util.suffixCounter("col_integer", Util.generateCounter())
        return listOf(
                //SimpleField.fixedLength(name(), FixedLengthType.TYPE_BOOLEAN),
                SimpleField.fixedLength(name(), FixedLengthType.TYPE_TINYINT),
                //SimpleField.fixedLength(name(), FixedLengthType.TYPE_SMALLINT),
                //SimpleField.fixedLength(name(), FixedLengthType.TYPE_INT),
                //SimpleField.fixedLength(name(), FixedLengthType.TYPE_BIGINT),
                SimpleField.fixedLength(name(), FixedLengthType.TYPE_LARGEINT)
        )
    }

    fun getDecimalV2Fields(): List<SimpleField> {
        return listOf(
                SimpleField.decimalv2("col_decimalv2", 27, 9)
        )
    }

    fun getFloatFields(): List<SimpleField> {
        val name = Util.suffixCounter("col_float", Util.generateCounter())
        return listOf(
                SimpleField.fixedLength(name(), FixedLengthType.TYPE_FLOAT),
                SimpleField.fixedLength(name(), FixedLengthType.TYPE_DOUBLE)
        )
    }

    fun getTimeFields(): List<SimpleField> {
        val name = Util.suffixCounter("col_time", Util.generateCounter())
        return listOf(
                SimpleField.fixedLength(name(), FixedLengthType.TYPE_DATE),
                SimpleField.fixedLength(name(), FixedLengthType.TYPE_DATETIME)
        )
    }

    fun getDecimalFields(): List<DecimalField> {
        val name = Util.suffixCounter("col_decimal1", Util.generateCounter())
        return listOf(
                //SimpleField.decimal(name(), 32, 9, 0),
                //SimpleField.decimal(name(), 32, 9, 2),
                SimpleField.decimal(name(), 32, 9, 9),
                //SimpleField.decimal(name(), 32, 7, 4),

                SimpleField.decimal(name(), 64, 18, 0),
                // SimpleField.decimal(name(), 64, 18, 2),
                // SimpleField.decimal(name(), 64, 18, 9),
                // SimpleField.decimal(name(), 64, 18, 18),
                // SimpleField.decimal(name(), 64, 15, 13),

                // SimpleField.decimal(name(), 128, 38, 0),
                // SimpleField.decimal(name(), 128, 38, 2),
                // SimpleField.decimal(name(), 128, 38, 9),
                // SimpleField.decimal(name(), 128, 38, 13),
                SimpleField.decimal(name(), 128, 38, 18)
                // SimpleField.decimal(name(), 128, 35, 30)
        )
    }

    fun getDecimalFields1(): List<DecimalField> {
        val name = Util.suffixCounter("col_decimal1", Util.generateCounter())
        return listOf(
                SimpleField.decimal(name(), 32, 7, 4),
                SimpleField.decimal(name(), 64, 15, 13),
                SimpleField.decimal(name(), 128, 35, 30)
        )
    }

    fun decimalToBool(value: BigDecimal): Boolean =
            value.unscaledValue() != BigInteger.ZERO

    fun decimalToInteger(value: BigDecimal, bits: Int): BigInteger? {
        val intValue = DecimalUtil.intPart(value).unscaledValue()!!
        val overflowCheck = DecimalUtil.integer_overflow_policy(bits)
        return overflowCheck(intValue)
    }

    fun fromDecimal(value: BigDecimal, fa: SimpleField, fb: SimpleField): String? {
        val sdfNoMs = SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        return when (fb) {
            is FixedLengthField -> when (fb.type) {
                FixedLengthType.TYPE_BOOLEAN -> decimalToBool(value).toString()
                FixedLengthType.TYPE_TINYINT -> decimalToInteger(value, 8)?.let { it.toString() }
                FixedLengthType.TYPE_SMALLINT -> decimalToInteger(value, 16)?.let { it.toString() }
                FixedLengthType.TYPE_INT -> decimalToInteger(value, 32)?.let { it.toString() }
                FixedLengthType.TYPE_BIGINT -> decimalToInteger(value, 64)?.let { it.toString() }
                FixedLengthType.TYPE_LARGEINT -> decimalToInteger(value, 128)?.let { it.toString() }
                FixedLengthType.TYPE_FLOAT -> value.toDouble().toFloat().toString()
                FixedLengthType.TYPE_DOUBLE -> value.toDouble().toString()
                FixedLengthType.TYPE_DATE -> decimalToInteger(value, 32)?.let { intToDate(it.toInt()).toString() }
                FixedLengthType.TYPE_DATETIME -> decimalToInteger(value, 64)?.let { sdfNoMs.format(longToTimestamp(it.toLong())) }
            }
            is DecimalV2Field -> decimalToDecimal(value, DecimalType(128, 27, 9))?.let { it.toString() }
            else -> null
        }
    }

    fun toType(value: Any, fa: SimpleField, fb: SimpleField): String? {
        when {
            fb is DecimalField -> {
                return toDecimal(value, DecimalType(fb.bits, fb.precision, fb.scale))?.let {
                    DecimalUtil.decimalToString(it)
                }
            }
            fa is DecimalField -> {
                return fromDecimal(value as BigDecimal, fa, fb)
            }
            else -> {
                return null
            }
        }
    }

    val genCodeFile = "decimal_cast.py"
    var genCode: PrintStream? = null
    val createSqlPrefix = "decimal_cast/sql"
    val dataCsvPrefix = "decimal_cast/csv"

    @BeforeClass
    fun setUp() {
        val genCodeFileObj = File(genCodeFile)
        if (genCodeFileObj.exists()) {
            genCodeFileObj.delete()
        }
        genCode = PrintStream(genCodeFileObj.outputStream())
        Util.recreateDir(createSqlPrefix)
        Util.recreateDir(dataCsvPrefix)
    }

    @AfterClass
    fun tearDown() {
        genCode!!.flush()
        genCode!!.close()
    }

    fun generateAllTest(fromToPairList: List<Pair<SimpleField, SimpleField>>, casesNum: Int = 50) {
        val cases = fromToPairList.flatMap { (fa, fb) ->
            val randGens = OrcUtil.getDefaultGenerators(listOf(fa))
            val gen = randGens.getValue(fa.name)
            val fromUniqueType = fa.uniqueType()
            val toUniqueType = fb.uniqueType();

            val name = "cast_${fromUniqueType}_as_${toUniqueType}".toLowerCase()
            val newFb = if (fb is DecimalField){
                val precision = when (fb.bits) {
                    128 -> {
                        38
                    }
                    64 -> {
                        18
                    }
                    else -> {
                        9
                    }
                }
                SimpleField.decimal(fb.name, fb.bits, precision, fb.scale)
            } else {
                fb
            }
            val fields = listOf(
                    SimpleField.fixedLength("seq", FixedLengthType.TYPE_INT),
                    fa.rename("src"),
                    CompoundField.nullable(newFb.rename("dst"), 50)
            )
            val table = Table(name, fields, 1)
            val values = Array(casesNum) { gen() }.toSet().toTypedArray()
            val counter = Util.generateCounter()
            val testCases = values.map { value ->
                val targetValue = toType(value, fa, fb)
                val seq = counter()
                listOf("$seq", "$value", targetValue?.let { "$targetValue" } ?: "NULL")
            }

            val createSqlPath = "$createSqlPrefix/${table.tableName}.sql"
            val dataCsvPath = "$dataCsvPrefix/${table.tableName}.csv"
            Util.createFile(File(createSqlPath), table.sql())
            Util.createFile(File(dataCsvPath), testCases.joinToString("\n") { it.joinToString(",") })
            val castExpr = "cast(src as ${fb.sqlType()})"
            Util.renderTemplate(
                    "decimal_cast_daily.template",
                    "name" to name,
                    "castExpr" to castExpr
            ).let { listOf(it) }
        }
        genCode!!.println(cases.joinToString("\n\n"))
    }

    fun timestampToStringWithoutMs(ts: Timestamp): String {
        val sdfNoMs = SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        return sdfNoMs.format(ts)
    }

    fun anyToString(value: Any, field: SimpleField): String {
        return when (field) {
            is FixedLengthField -> when (field.type) {
                FixedLengthType.TYPE_DATETIME -> timestampToStringWithoutMs(value as Timestamp)
                else -> value.toString()
            }
            else -> value.toString()
        }
    }

    fun generateAllTestReverse(fromFields: List<SimpleField>, toFields: List<SimpleField>, category: String, filename: String, casesNum: Int = 20) {
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

                val name = "cast_${toUniqueType}_as_${fromUniqueType}".toLowerCase()
                val fields = listOf(
                        SimpleField.fixedLength("seq", FixedLengthType.TYPE_INT),
                        fb.rename("src"),
                        CompoundField.nullable(fa.rename("dst"), 50)
                )
                val table = Table(name, fields, 1)
                val values = Array(casesNum) { gen() }.toSet().toTypedArray()
                val counter = Util.generateCounter()
                val testCases = values.map { value ->
                    val targetValue = toType(value, fa, fb)
                    val seq = counter()
                    listOf("$seq", targetValue?.let { "$targetValue" } ?: "NULL", "$value")
                }.filter { t -> t[1] != "NULL" }

                val createSqlPath = "$createSqlPrefix/${table.tableName}.sql"
                val dataCsvPath = "$dataCsvPrefix/${table.tableName}.csv"
                Util.createFile(File(createSqlPath), table.sql())
                Util.createFile(File(dataCsvPath), testCases.joinToString("\n") { it.joinToString(",") })
                val castExpr = "cast(src as ${fa.sqlType()})"
                Util.renderTemplate(
                        "decimal_cast_daily.template",
                        "name" to name,
                        "castExpr" to castExpr
                ).let { listOf(it) }
            }
        }
        genCode!!.println(cases.joinToString("\n\n"))
    }

    /*
    @Test
    fun test() {
        val tcList = listOf(
                CastTestCase(-1, -1, "0.34", 9, 2, "0.34", false),
                CastTestCase(-1, -1, "0.35", 9, 2, "0.34", false)
        )
        println(generateTest(tcList, "TYPE_FLOAT", "TYPE_DECIMAL32", "testCastOfFloatAndDecimal32", "Float", ""))
    }
     */


    @Test
    fun test1(){
        val s="1.218172000197920506"
        val srcField = SimpleField.decimal("src", 128, 38, 18)
        val dstField = SimpleField.decimal("dst", 32, 9, 9)
        val value = BigDecimal(s)
        println(toType(value, srcField, dstField))
        val s1="1.882742171651175826"
        println(toType(BigDecimal(s1), srcField, dstField))
    }

    @Test
    fun testAllIntegerToDecimalUT() {
        generateAllTest(Util.crossProduct(getIntegerFields(), getDecimalFields1()))
    }

    @Test
    fun testAllFloatToDecimalUT() {
        generateAllTest(Util.crossProduct(getFloatFields(), getDecimalFields1()))
    }

    // @Test
    // fun testAllTimeToDecimalUT() {
    //     generateAllTest(getTimeFields(), getDecimalFields(), "Time", "cast_from_time_to_decimal")
    // }

    @Test
    fun testAllDecimalToDecimalUT() {
        generateAllTest(Util.crossProduct(getDecimalFields(), getDecimalFields()))
    }

    @Test
    fun testAllDecimalV2ToDecimalUT() {
        generateAllTest(Util.crossProduct(getDecimalV2Fields(), getDecimalFields1()))
    }

    @Test
    fun testAllDecimalToIntegerUT() {
        generateAllTest(Util.crossProduct(getDecimalFields1(), getIntegerFields()))
    }

    @Test
    fun testAllDecimalToFloatUT() {
        generateAllTest(Util.crossProduct(getDecimalFields1(), getFloatFields()))
    }

    // @Test
    // fun testAllDecimalToTimeUT() {
    //     generateAllTestReverse(getTimeFields(), getDecimalFields(), "Time", "cast_from_decimal_to_time")
    // }

    @Test
    fun testAllDecimalToDecimalV2UT() {
        generateAllTest(Util.crossProduct(getDecimalFields1(), getDecimalV2Fields()))
    }

    /*
    fun testFloatToDecimal32p9s2() {
        val a: Float = -0.88404423F
        val d = toDecimal(a, DecimalType(128, 38, 0))
        //println(DecimalUtil.doubleToRawDouble(a.toDouble()))
        println(d)
    }

    fun testDateToInt() {
        val genDate = RandUtil.generateRandomDate("1000-12-12", "9999-12-12")
        Array(1000) { genDate() }.forEach { d ->
            val v0 = d.toString().replace("-", "").toInt()
            val v1 = dateToInt(d)
            println("date0=$d, date=$v0, v=$v1")
            Assert.assertEquals(v0, v1)
        }
    }

    fun testDateToInt2() {
        arrayOf("0756-10-15").forEach {
            val d = Date.valueOf(it)
            println("date=$d")
            val v = dateToInt(d)
            println("int=$v")
            val v0 = d.toString().replace("-", "").toInt()
            println("v0=$v0")
        }
    }

    fun testTimestampToLong() {
        val genTimestamp = RandUtil.generateRandomTimestamp("1000-12-12 00:00:00", "9999-12-12 00:00:00")
        val sdf = SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
        val sdfNoMs = SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        Array(1000) { genTimestamp() }.forEach { d ->
            val v0 = d.toString().replace(Regex("[ :-]"), "").replace(Regex("\\.\\d+$"), "").toLong()
            val v1 = timestampToLong(d)
            println("date0=$d, date=$v0, v=$v1")
            val ds = sdf.format(d)
            val dsNoMs = sdfNoMs.format(d)
            println("ds=$ds, dsNoMs=$dsNoMs")
            Assert.assertEquals(v0, v1)
        }
    }

    fun testDecimal128p38s0ToDecimalv2Normal() {
        val bigInteger = BigInteger("70141183460469231731687303715884105728")
        val decimal = BigDecimal(bigInteger, 0)
        val targetValue = toType(decimal, SimpleField.decimal("col0", 128, 38, 0),
                SimpleField.decimalv2("col1", 27, 9))
        Assert.assertEquals(targetValue, null)
        println(targetValue)
    }

    fun testDecimal32p9s2ToTinyInt() {
        val decimal = BigDecimal(BigInteger("22"), 0)
        val targetValue = toType(decimal, SimpleField.decimal("col1", 32, 9, 0),
                SimpleField.fixedLength("col1", FixedLengthType.TYPE_TINYINT))
        println(targetValue)
        Assert.assertTrue(targetValue != null)
        Assert.assertTrue(targetValue == "22")
    }

    fun testBigDecimalScaleAndPrecision(){
        val ds = arrayOf(
                "0",
                "0.000",
                "3.14",
                "3.1415926",
                "3.1400000",
                "314.15",
                "314.15000",
                "0",
                "0.000",
                "-3.14",
                "-3.1415926",
                "-314.15",
                "-314.15000"
        )
        ds.map { BigDecimal(it).stripTrailingZeros() }.forEach { d->
            println("d=${d.toPlainString()}, precision=${d.precision()}, scale=${d.scale()}")
        }
    }

    fun randomString(){
        val gen = RandUtil.generateRandomDate("2021-01-01", "2021-01-31")
        val dateFMT = SimpleDateFormat("yyyy-MM-dd")
        (0..10).forEach {
            val time = (gen() as Date).time
            val date = Date();
            date.time = time
            println(dateFMT.format(date))
        }
    }

     */
}
