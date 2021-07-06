package com.grakra.schema

abstract sealed class Field(open val name: String) {
    abstract fun sqlType(): String
    open fun hiveSqlType(): String = sqlType()
    open fun sql(): String = "$name ${sqlType()} NOT NULL"
    open fun hiveSql(): String = "$name ${hiveSqlType()}"
    fun simple(): SimpleField {
        return when (this) {
            is CompoundField -> {
                this.fld
            }
            is SimpleField -> {
                this
            }
            is AggregateField -> this.fld.simple()
        }
    }
}

abstract sealed class SimpleField(override val name: String) : Field(name) {
    companion object {
        fun fixedLength(name: String, type: FixedLengthType): SimpleField = FixedLengthField(name, type)
        fun char(name: String, len: Int): SimpleField = CharField(name, len)
        fun varchar(name: String, len: Int): SimpleField = VarCharField(name, len)
        fun decimal(name: String, bits: Int, precision: Int, scale: Int) = DecimalField(name, bits, precision, scale)
        fun decimalv2(name: String, precision: Int, scale: Int) = DecimalV2Field(name, precision, scale)
    }

    fun rename(newName: String): SimpleField {
        return when (this) {
            is FixedLengthField -> fixedLength(newName, this.type)
            is CharField -> char(newName, this.len)
            is VarCharField -> varchar(newName, this.len)
            is DecimalField -> decimal(newName, this.bits, this.precision, this.scale)
            is DecimalV2Field -> decimalv2(newName, this.precision, this.scale)
        }
    }

    fun precisionAndScale(): Pair<Int, Int> {
        return when (this) {
            is DecimalField -> this.precision to this.scale
            is DecimalV2Field -> this.precision to this.scale
            else -> -1 to -1
        }
    }

    fun primitiveType(): String {
        return when (this) {
            is FixedLengthField -> this.type.toString()
            is DecimalV2Field -> "TYPE_DECIMALV2"
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
        return when (this) {
            is FixedLengthField -> typ
            is CharField -> "$typ(${this.len})"
            is VarCharField -> "$typ(${this.len})"
            is DecimalField -> "DECIMAL${this.bits}(${this.precision}, ${this.scale})"
            is DecimalV2Field -> "DECIMALV2(${this.precision}, ${this.scale})"
        }
    }

    override fun hiveSqlType(): String {
        if (this is FixedLengthField && this.type == FixedLengthType.TYPE_DATETIME) {
            return "TIMESTAMP"
        }
        return when (this) {
            is FixedLengthField -> when (type) {
                FixedLengthType.TYPE_DATETIME -> "TIMESTAMP"
                else -> sqlType()
            }
            is CharField -> "STRING"
            is VarCharField -> "STRING"
            is DecimalField -> "DECIMAL(${this.precision}, ${this.scale})"
            else -> sqlType()
        }
    }

    fun hivePartitionKeySql() = "$name ${hiveSqlType()}"
}

abstract sealed class CompoundField(open val fld: SimpleField) : Field(fld.name) {
    companion object {
        fun trivial(fld: SimpleField): CompoundField = TrivialCompoundField(fld)
        fun nullable(fld: SimpleField, nullRatio: Int): CompoundField = NullableField(fld, nullRatio)
        fun default_value(fld: SimpleField, value: String): CompoundField = DefaultValueField(fld, value)
        fun nullable_default_value(fld: SimpleField, nullRatio: Int, value: String): CompoundField = NullableDefaultValueField(fld, nullRatio, value)
    }

    override fun sqlType(): String = fld.sqlType()
    override fun hiveSqlType(): String = fld.hiveSqlType()

    override fun sql(): String {
        val colDef = this.fld.sql().replace(Regex("NOT\\s+NULL"), "")
        return when (this) {
            is TrivialCompoundField -> fld.sql()
            is NullableField -> "$colDef NULL"
            is DefaultValueField -> "${this.fld.sql()} DEFAULT \"${this.value}\""
            is NullableDefaultValueField -> "$colDef NULL DEFAULT \"${this.value}\""
        }
    }

    override fun hiveSql(): String {
        val colDef = this.fld.hiveSql()
        return when (this) {
            is TrivialCompoundField -> colDef
            is NullableField -> colDef
            is DefaultValueField -> "$colDef DEFAULT \"${this.value}\""
            is NullableDefaultValueField -> "$colDef DEFAULT \"${this.value}\""
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
}

enum class AggregateType {
    NONE,
    MIN,
    SUM,
    MAX,
    HLL_UNION,
    BITMAP_UNION,
    REPLACE,
    REPLACE_IF_NOT_NULL,
    UNKNOWN,
    PERCENTILE_UNION,
}

data class FixedLengthField(override val name: String, val type: FixedLengthType) : SimpleField(name) {}
data class CharField(override val name: String, val len: Int) : SimpleField(name) {}
data class VarCharField(override val name: String, val len: Int) : SimpleField(name) {}
data class DecimalField(override val name: String, val bits: Int, val precision: Int, val scale: Int) : SimpleField(name) {}
data class DecimalV2Field(override val name: String, val precision: Int, val scale: Int) : SimpleField(name) {}
data class NullableField(override val fld: SimpleField, val nullRatio: Int) : CompoundField(fld) {}
data class TrivialCompoundField(override val fld: SimpleField) : CompoundField(fld) {}
data class DefaultValueField(override val fld: SimpleField, val value: String) : CompoundField(fld) {}
data class NullableDefaultValueField(override val fld: SimpleField, val nullRatio: Int, val value: String) : CompoundField(fld) {}

data class AggregateField(val fld: CompoundField, val aggType: AggregateType) : Field(fld.name) {
    companion object {
        fun aggregate(fld: CompoundField, aggType: AggregateType) = AggregateField(fld, aggType)
    }

    override fun sqlType(): kotlin.String {
        return when (aggType) {
            AggregateType.NONE -> fld.sqlType()
            else -> fld.sqlType() + " " + aggType.name
        }
    }

    override fun sql(): String {
        if (aggType == AggregateType.NONE) {
            return this.fld.sql()
        }

        val colDef = this.fld.fld.sql().replace(Regex("NOT\\s+NULL"), "")
        return when (this.fld) {
            is TrivialCompoundField -> "$colDef ${aggType.name} NOT NULL"
            is NullableField -> "$colDef ${aggType.name} NULL"
            is DefaultValueField -> "$colDef ${aggType.name} DEFAULT \"${this.fld.value}\""
            is NullableDefaultValueField -> "$colDef ${aggType.name} NULL DEFAULT \"${this.fld.value}\""
        }
    }
}
