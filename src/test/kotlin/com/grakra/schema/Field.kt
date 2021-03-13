package com.grakra.schema

abstract sealed class Field(open val name: String) {
    abstract fun sqlType(): String
    fun sql(): String = "$name ${sqlType()}"
}

abstract sealed class SimpleField(override val name: String) : Field(name) {
    companion object {
        fun fixedLength(name: String, type: FixedLengthType): SimpleField = FixedLengthField(name, type)
        fun char(name: String, len: Int): SimpleField = CharField(name, len)
        fun varchar(name: String, len: Int): SimpleField = VarCharField(name, len)
        fun decimal(name: String, bits: Int, precision: Int, scale: Int) = DecimalField(name, bits, precision, scale)
    }

    fun precisionAndScale(): Pair<Int, Int> {
        return when (this) {
            is FixedLengthField -> {
                when (this.type) {
                    FixedLengthType.TYPE_DECIMALV2 -> 27 to 9
                    else -> -1 to -1
                }
            }
            is DecimalField -> this.precision to this.scale
            else -> -1 to -1
        }
    }

    fun primitiveType(): String {
        return when (this) {
            is FixedLengthField -> this.type.toString()
            is DecimalField -> "TYPE_DECIMAL${this.bits}"
            is CharField -> "TYPE_CHAR"
            is VarCharField -> "TYPE_VARCHAR"
        }
    }

    fun uniqueType(): String {
        val (p, s) = precisionAndScale()
        val pType = primitiveType().substring("TYPE_".length).toLowerCase().capitalize()
        return when (this) {
            is FixedLengthField -> when (this.type) {
                FixedLengthType.TYPE_TINYINT -> "TinyInt"
                FixedLengthType.TYPE_SMALLINT -> "SmallInt"
                FixedLengthType.TYPE_BIGINT -> "BigInt"
                FixedLengthType.TYPE_LARGEINT -> "LargeInt"
                else -> pType
            }
            is DecimalField -> "${pType}p${p}s${s}"
            else -> pType
        }
    }

    override fun sqlType(): String {
        val typ = this.uniqueType().toUpperCase()
        val colDef = when (this) {
            is FixedLengthField -> when (this.type) {
                FixedLengthType.TYPE_DECIMALV2 -> "DECIMAL(27,9)"
                else -> typ
            }
            is CharField -> "$typ(${this.len})"
            is VarCharField -> "$typ(${this.len})"
            is DecimalField -> "DECIMAL${this.bits}(${this.precision}, ${this.scale})"
        }
        return "$colDef NOT NULL"
    }
}

abstract sealed class CompoundField(open val fld: Field) : Field(fld.name) {
    companion object {
        fun nullable(fld: Field, nullRatio: Int): CompoundField = NullableField(fld, nullRatio)
        fun default_value(fld: Field, value: String): CompoundField = DefaultValueField(fld, value)
        fun nullable_default_value(fld: Field, nullRatio: Int, value: String): CompoundField = NullableDefaultValueField(fld, nullRatio, value)
    }

    override fun sqlType(): String {
        val colDef = this.fld.sqlType().replace(Regex("NOT\\s+NULL"), "")
        return when (this) {
            is NullableField -> "$colDef NULL"
            is DefaultValueField -> "${this.fld.sqlType()} DEFAULT \"${this.value}\""
            is NullableDefaultValueField -> "$colDef NULL DEFAULT \"${this.value}\""
        }
    }
}

enum class FixedLengthType {
    TYPE_BOOLEAN,
    TYPE_TINYINT,
    TYPE_SMALLINT,
    TYPE_INT,
    TYPE_BIGINT,
    TYPE_LARGEINT,
    TYPE_FLOAT,
    TYPE_DOUBLE,
    TYPE_DATE,
    TYPE_DATETIME,
    TYPE_DECIMALV2,
}

data class FixedLengthField(override val name: String, val type: FixedLengthType) : SimpleField(name) {}
data class CharField(override val name: String, val len: Int) : SimpleField(name) {}
data class VarCharField(override val name: String, val len: Int) : SimpleField(name) {}
data class DecimalField(override val name: String, val bits: Int, val precision: Int, val scale: Int) : SimpleField(name) {}
data class NullableField(override val fld: Field, val nullRatio: Int) : CompoundField(fld) {}
data class DefaultValueField(override val fld: Field, val value: String) : CompoundField(fld) {}
data class NullableDefaultValueField(override val fld: Field, val nullRatio: Int, val value: String) : CompoundField(fld) {}
