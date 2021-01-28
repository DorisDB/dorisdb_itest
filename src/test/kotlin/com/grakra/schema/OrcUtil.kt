package com.grakra.schema

import com.grakra.schema.FixedLengthType.*
import com.grakra.util.RandUtil
import com.grakra.util.Util
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.common.type.HiveDecimal
import org.apache.hadoop.hive.ql.exec.vector.*
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable
import org.apache.orc.OrcFile
import org.apache.orc.TypeDescription
import java.io.File
import java.math.BigDecimal
import java.nio.file.Files
import java.sql.Date
import java.sql.Timestamp

object OrcUtil {
    fun field2OrcTypeDecription(f: Field): TypeDescription {
        return when (f) {
            is SimpleField -> when (f) {
                is FixedLengthField -> when (f.type) {
                    TYPE_BOOLEAN -> TypeDescription.createBoolean()
                    TYPE_TINYINT -> TypeDescription.createByte()
                    TYPE_SMALLINT -> TypeDescription.createShort()
                    TYPE_INT -> TypeDescription.createInt()
                    TYPE_BIGINT -> TypeDescription.createLong()
                    TYPE_LARGEINT -> TypeDescription.createLong()
                    TYPE_FLOAT -> TypeDescription.createFloat()
                    TYPE_DOUBLE -> TypeDescription.createDouble()
                    TYPE_DATE -> TypeDescription.createDate()
                    TYPE_DATETIME -> TypeDescription.createTimestamp()
                    TYPE_DECIMALV2 -> TypeDescription.createDecimal().withScale(9).withPrecision(27)
                }
                is CharField -> TypeDescription.createChar().withMaxLength(f.len)
                is VarCharField -> TypeDescription.createChar().withMaxLength(f.len)
                is DecimalField -> TypeDescription.createDecimal().withScale(f.scale).withPrecision(f.precision)
            }
            is CompoundField -> field2OrcTypeDecription(f.fld)
        }
    }

    fun createOrcSchemaFromFields(fields: List<Field>): TypeDescription {
        val schema = TypeDescription.createStruct()
        fields.forEach { f ->
            schema.addField(f.name, field2OrcTypeDecription(f))
        }
        return schema
    }

    fun field2DefaultGenerator(f: Field): () -> Any {
        return when (f) {
            is SimpleField -> when (f) {
                is FixedLengthField -> when (f.type) {
                    TYPE_BOOLEAN -> RandUtil.generateRandomBoolean(50)
                    TYPE_TINYINT -> RandUtil.generateRandomTinyInt(50)
                    TYPE_SMALLINT -> RandUtil.generateRandomSmallInt(50)
                    TYPE_INT -> RandUtil.generateRandomInt(50)
                    TYPE_BIGINT -> RandUtil.generateRandomBigInt(50)
                    TYPE_LARGEINT -> RandUtil.generateRandomLargeInt(50)
                    TYPE_FLOAT -> RandUtil.generateRandomFloat()
                    TYPE_DOUBLE -> RandUtil.generateRandomDouble()
                    TYPE_DATE -> RandUtil.generateRandomDate("1990-01-01", "2021-12-31")
                    TYPE_DATETIME -> RandUtil.generateRandomTimestamp("2001-01-01 00:00:00", "2021-12-31 00:00:00")
                    TYPE_DECIMALV2 -> RandUtil.generateRandomDecimal(27, 9, 50)
                }
                is CharField -> RandUtil.generateRandomVarChar(RandUtil.lc() + RandUtil.uc() + RandUtil.digit(), 0, 255)
                is VarCharField -> RandUtil.generateRandomVarChar(RandUtil.lc() + RandUtil.uc() + RandUtil.digit(), 0, 255)
                is DecimalField -> RandUtil.generateRandomDecimal(f.precision, f.scale, 50)
            }
            is CompoundField -> field2DefaultGenerator(f.fld)
        }
    }

    fun getDefaultGenerators(fields: List<Field>): Map<String, () -> Any> {
        return fields.map { f -> f.name to field2DefaultGenerator(f) }.toMap()
    }

    fun largeIntField(f: Field): FixedLengthField? {
        return when (f) {
            is FixedLengthField -> if (f.type == TYPE_LARGEINT) {
                f
            } else {
                null
            }
            is CompoundField -> largeIntField(f.fld)
            else -> null;
        }
    }

    fun getNamesOfLargeIntFields(fields: List<Field>): List<String> {
        return fields.mapNotNull { f ->
            largeIntField(f)?.name
        }.toList()
    }

    fun getDefaultGeneratorsForOrc(fields: List<Field>): Map<String, () -> Any> {
        val generators = getDefaultGenerators(fields)
        val largeIntFields = getNamesOfLargeIntFields(fields).toSet()
        return generators.map { (name, gen) ->
            if (largeIntFields.contains(name)) {
                name to RandUtil.generateRandomBigInt(50)
            } else {
                name to gen
            }
        }.toMap()
    }



    fun generateSetOrcCell(cv: ColumnVector, f: Field, generator: () -> Any, chunkMaxSize: Int): () -> Unit {
        val idx = Util.generateCounter(chunkMaxSize)
        when (f) {
            is FixedLengthField -> when (f.type) {
                TYPE_BOOLEAN ->
                    return {
                        (cv as LongColumnVector).vector[idx()] = if ((generator() as Boolean)) {
                            1L
                        } else {
                            0L
                        }
                    }
                TYPE_TINYINT ->
                    return {
                        (cv as LongColumnVector).vector[idx()] = (generator() as Byte).toLong()
                    }
                TYPE_SMALLINT ->
                    return {
                        (cv as LongColumnVector).vector[idx()] = (generator() as Short).toLong()
                    }

                TYPE_INT ->
                    return {
                        (cv as LongColumnVector).vector[idx()] = (generator() as Int).toLong()
                    }

                TYPE_BIGINT ->
                    return {
                        (cv as LongColumnVector).vector[idx()] = (generator() as Long).toLong()
                    }

                TYPE_LARGEINT ->
                    return {
                        (cv as LongColumnVector).vector[idx()] = (generator() as Long).toLong()
                    }

                TYPE_FLOAT ->
                    return {
                        (cv as DoubleColumnVector).vector[idx()] = (generator() as Float).toDouble()
                    }

                TYPE_DOUBLE ->
                    return {
                        (cv as DoubleColumnVector).vector[idx()] = (generator() as Double).toDouble()
                    }

                TYPE_DATE ->
                    return {
                        (cv as LongColumnVector).vector[idx()] = (generator() as Date).time
                    }
                TYPE_DATETIME ->
                    return {
                        (cv as TimestampColumnVector).set(idx(), generator() as Timestamp)
                    }
                TYPE_DECIMALV2 ->
                    return {
                        (cv as DecimalColumnVector).vector[idx()] = HiveDecimalWritable(
                                HiveDecimal.create(generator() as BigDecimal))
                    }
            }
            is CharField, is VarCharField ->
                return {
                    (cv as BytesColumnVector).setVal(idx(), generator() as ByteArray)
                }
            is DecimalField ->
                return {
                    (cv as DecimalColumnVector).vector[idx()] = HiveDecimalWritable(
                            HiveDecimal.create(generator() as BigDecimal))
                }
            is NullableField -> {
                val setNull = RandUtil.generateRandomBoolean(f.nullRatio)
                val nullIdx = Util.generateCounter(chunkMaxSize)
                val setCell = generateSetOrcCell(cv, f.fld, generator, chunkMaxSize)
                return {
                    val isNull = setNull()
                    cv.isNull[nullIdx()] = isNull
                    if (isNull) {
                        cv.noNulls = false
                    }
                    setCell()
                }
            }
            is NullableDefaultValueField -> {
                val setNull = RandUtil.generateRandomBoolean(f.nullRatio)
                val nullIdx = Util.generateCounter(chunkMaxSize)
                val setCell = generateSetOrcCell(cv, f.fld, generator, chunkMaxSize)
                return {
                    val isNull = setNull()
                    cv.isNull[nullIdx()] = isNull
                    if (isNull) {
                        cv.noNulls = false
                    }
                    setCell()
                }
            }
            is DefaultValueField -> return generateSetOrcCell(cv, f.fld, generator, chunkMaxSize)
        }
    }

    fun generateAppendChunk(fields: List<Field>, chunkMaxSize: Int): (Int) -> VectorizedRowBatch {
        val schema = createOrcSchemaFromFields(fields)
        val rowBatch = schema.createRowBatch(chunkMaxSize)
        val generators = getDefaultGeneratorsForOrc(fields)
        val fieldArray = fields.toTypedArray()
        val setCells = (0 until rowBatch.numCols).map { c ->
            generateSetOrcCell(rowBatch.cols[c], fieldArray[c], generators.getValue(fieldArray[c].name), chunkMaxSize)
        }
        val setAllCells = { setCells.forEach { it() } }
        return { chunkSize ->
            rowBatch.reset()
            (0 until chunkSize).forEach { _ ->
                setAllCells()
                rowBatch.size++
            }
            rowBatch
        }
    }

    fun createOrcFile(path: String, fields: List<Field>, rowsNum: Int, maxChunkSize: Int) {
        val conf = Configuration()
        val dfsPath = org.apache.hadoop.fs.Path(path)
        val file = File(path)
        val schema = createOrcSchemaFromFields(fields);
        if (Files.exists(file.toPath())) {
            file.delete()
        }
        val writer = OrcFile.createWriter(dfsPath, OrcFile.writerOptions(conf).setSchema(schema))
        val generators = getDefaultGeneratorsForOrc(fields)
        val rowBatch = schema.createRowBatch()
        val appendChunk = generateAppendChunk(fields, maxChunkSize)
        val chunkSizes = Array(rowsNum / maxChunkSize) { maxChunkSize } + arrayOf(rowsNum % maxChunkSize)
        chunkSizes.forEach { chunkSize ->
            writer.addRowBatch(appendChunk(chunkSize))
        }
        writer.close()
    }

    fun nthItemOfColumnWithNullCheck(vector: ColumnVector, desc: TypeDescription): (i: Int) -> String {
        val nthItem = nthItemOfColumn(vector, desc)
        return { i ->
            if (!vector.noNulls && vector.isNull[i]) {
                "NULL"
            } else {
                nthItem(i)
            }
        }
    }

    fun nthItemOfColumn(vector: ColumnVector, desc: TypeDescription): (i: Int) -> String {
        return { i ->
            when (desc.category) {
                TypeDescription.Category.STRING,
                TypeDescription.Category.VARCHAR,
                TypeDescription.Category.CHAR,
                TypeDescription.Category.BINARY -> {
                    val binaryVector = vector as BytesColumnVector
                    String(binaryVector.vector[i], Charsets.UTF_8).take(10) + "..."
                }
                TypeDescription.Category.BOOLEAN,
                TypeDescription.Category.BYTE,
                TypeDescription.Category.SHORT,
                TypeDescription.Category.INT,
                TypeDescription.Category.LONG -> {
                    val byteVector = vector as LongColumnVector
                    byteVector.vector[i].toString()
                }
                TypeDescription.Category.FLOAT,
                TypeDescription.Category.DOUBLE -> {
                    val byteVector = vector as DoubleColumnVector
                    byteVector.vector[i].toString()
                }
                TypeDescription.Category.DATE -> {
                    val dateVector = vector as LongColumnVector
                    dateVector.vector[i].toString()
                }
                TypeDescription.Category.TIMESTAMP -> {
                    val datetimeVector = vector as TimestampColumnVector
                    datetimeVector.getTime(i).toString()
                }
                TypeDescription.Category.DECIMAL -> {
                    val decimalVector = vector as DecimalColumnVector
                    decimalVector.vector[i].hiveDecimal.toFormatString(desc.scale)
                }
                TypeDescription.Category.LIST -> TODO()
                TypeDescription.Category.MAP -> TODO()
                TypeDescription.Category.STRUCT -> TODO()
                TypeDescription.Category.UNION -> TODO()
            }
        }
    }

    fun readOrcFile(path: String) {
        val conf = Configuration()
        val dfsPath = org.apache.hadoop.fs.Path(path)
        val reader = OrcFile.createReader(dfsPath, OrcFile.ReaderOptions(conf))
        println(reader.schema)
        val rowBatch = reader.schema.createRowBatch()
        val columnFormatterGenerators = Array(reader.schema.fieldNames.size) {
            { vector: ColumnVector, desc: TypeDescription -> nthItemOfColumnWithNullCheck(vector, desc) }
        }
        val columnFormatters = Array(reader.schema.fieldNames.size) {
            { _: Int -> "" }
        }
        val rows = reader.rows()
        while (rows.nextBatch(rowBatch)) {
            for (c: Int in 0 until rowBatch.numCols) {
                columnFormatters[c] = columnFormatterGenerators[c](rowBatch.cols[c], reader.schema.findSubtype(c + 1))
            }
            for (i: Int in 0 until rowBatch.size) {
                for (c: Int in 0 until rowBatch.numCols) {
                    print("${columnFormatters[c](i.toInt())},\t")
                }
                println()
            }
        }
        rows.close()
    }
}