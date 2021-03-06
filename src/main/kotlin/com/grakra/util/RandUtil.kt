package com.grakra.util

import com.google.common.base.Strings
import java.math.BigDecimal
import java.math.BigInteger
import java.sql.Date
import java.sql.Timestamp
import java.util.*
import kotlin.math.abs

object RandUtil {
    fun generateRandomInteger(bits: Int, negRatio: Int): () -> BigInteger {
        val rand = Random()
        val max = DecimalUtil.max_bin_integer(bits)
        val min = DecimalUtil.min_bin_integer(bits)
        return {
            if (rand.nextInt(100) < negRatio) {
                when (val randBits = rand.nextInt(bits)) {
                    bits - 1 -> min
                    else -> {
                        BigInteger.ONE.shiftLeft(randBits).add(BigInteger(randBits, rand)).negate()
                    }
                }
            } else {
                when (val randBits = rand.nextInt(bits + 1)) {
                    0 -> {
                        BigInteger.ZERO
                    }
                    bits -> {
                        max
                    }
                    else -> {
                        BigInteger.ONE.shiftLeft(randBits - 1).add(BigInteger(randBits - 1, rand))
                    }
                }
            }
        }
    }

    fun generateRandomDecimalBinary(precision:Int, scale:Int):()->ByteArray {
        val decimalGen = generateRandomDecimal(precision, scale, 50)
        return {
            decimalGen().toPlainString().toByteArray()
        }
    }

    fun generateRandomBoolean(falseRatio: Int): () -> Boolean {

        val r = generateRandomInt(falseRatio)
        return {
            r() > 0
        }
    }


    fun getFiniteSetGenerator(n: Int, generator: () -> Any): () -> Any {
        val finiteSet = mutableSetOf<Any>()
        val rand = Random()
        val setSize = n / 2 + rand.nextInt(n)
        while (finiteSet.size < setSize) {
            finiteSet.add(generator())
        }
        val finiteSetArray = finiteSet.toTypedArray()
        return {
            finiteSetArray[rand.nextInt(setSize)]
        }
    }

    fun generateRandomTinyInt(negRatio: Int): () -> Byte {
        val r = generateRandomInteger(8, negRatio)
        return { r().toByte() }
    }

    fun generateRandomSmallInt(negRatio: Int): () -> Short {
        val r = generateRandomInteger(16, negRatio)
        return { r().toShort() }
    }

    fun generateRandomInt(negRatio: Int): () -> Int {
        val r = generateRandomInteger(32, negRatio)
        return { r().toInt() }
    }

    fun generateRandomBigInt(negRatio: Int): () -> Long {
        val r = generateRandomInteger(64, negRatio)
        return { r().toLong() }
    }

    fun generateRandomLargeInt(negRatio: Int): () -> BigInteger =
            generateRandomInteger(128, negRatio)


    fun generateRandomDecimal(precision: Int, scale: Int, negRatio: Int): () -> BigDecimal {
        val maxValue = BigInteger(Strings.repeat("9", precision))
        val randomBigInt = generateRandomLargeInt(negRatio)
        return {
            val bigInt = randomBigInt()
            if (bigInt.signum() < 0) {
                BigDecimal(bigInt.mod(maxValue), scale).negate()
            } else {
                BigDecimal(bigInt.mod(maxValue), scale)
            }
        }
    }

    fun generateRandomVarChar(alphabeta: Array<Char>, minLength: Int, maxLength: Int): () -> ByteArray {
        val rand = Random()
        return {
            val delta = if (minLength == maxLength) {
                0
            } else {
                rand.nextInt(maxLength - minLength)
            }

            val clob = CharArray(minLength + delta) {
                alphabeta[rand.nextInt(alphabeta.size)]
            }
            String(clob).toByteArray(Charsets.UTF_8)
        }
    }
    fun generateVarchar(minLength:Int, maxLength:Int):()->ByteArray {
        return generateRandomVarChar(lc()+ uc()+ digit(), minLength, maxLength)
    }

    fun lc() = (0..25).map { ('a'.toInt() + it).toChar() }.toTypedArray()
    fun uc() = (0..25).map { ('A'.toInt() + it).toChar() }.toTypedArray()
    fun digit() = (0..9).map { ('0'.toInt() + it).toChar() }.toTypedArray()
    fun extraXdigit() =
            (0..5).map { ('A'.toInt() + it).toChar() }.toTypedArray() +
                    (0..5).map { ('a'.toInt() + it).toChar() }.toTypedArray()

    fun xdigit() = digit() + extraXdigit()

    fun generateRandomFloat(): () -> Float {
        val rand = Random()
        val genInt = generateRandomInt(50)
        return {
            rand.nextFloat() * genInt()
        }
    }

    fun generateRandomDouble(): () -> Double {
        val rand = Random()
        val genLong = generateRandomBigInt(50)
        return {
            rand.nextDouble() * genLong()
        }
    }

    fun generateRandomDate(start: String, end: String): () -> Date {
        val rand = Random()
        val startDay = Date.valueOf(start).time / 86400000
        val endDay = Date.valueOf(end).time / 86400000

        return {
            val date = Date((startDay + abs(rand.nextLong() % (endDay - startDay))) * 86400000)
            date
        }
    }

    fun generateRandomTimestamp(start: String, end: String): () -> Timestamp {
        val rand = Random()
        val startMs = Timestamp.valueOf(start).time
        val endMs = Timestamp.valueOf(end).time
        return {
            Timestamp((startMs + abs(rand.nextLong() % (endMs - startMs))))
        }
    }
}