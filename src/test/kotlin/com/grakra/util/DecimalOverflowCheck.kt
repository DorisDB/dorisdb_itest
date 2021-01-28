package com.grakra.util

import com.google.common.base.Strings
import java.math.BigDecimal
import java.math.BigInteger

object DecimalOverflowCheck {
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
}