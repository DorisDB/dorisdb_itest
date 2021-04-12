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
import java.io.OutputStream
import java.io.PrintStream
import java.math.BigDecimal
import java.nio.file.Files
import java.sql.Date
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.*

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
                }
                is CharField -> TypeDescription.createChar().withMaxLength(f.len)
                is VarCharField -> TypeDescription.createChar().withMaxLength(f.len)
                is DecimalField -> TypeDescription.createDecimal().withScale(f.scale).withPrecision(f.precision)
                is DecimalV2Field -> TypeDescription.createDecimal().withScale(f.scale).withPrecision(f.precision)
            }
            is CompoundField -> field2OrcTypeDecription(f.fld)
            is AggregateField -> field2OrcTypeDecription(f.fld)
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
                }
                is CharField -> RandUtil.generateRandomVarChar(RandUtil.lc() + RandUtil.uc() + RandUtil.digit(), 0, f.len)
                is VarCharField -> RandUtil.generateRandomVarChar(RandUtil.lc() + RandUtil.uc() + RandUtil.digit(), 0, f.len)
                is DecimalField -> RandUtil.generateRandomDecimal(f.precision, f.scale, 50)
                is DecimalV2Field -> RandUtil.generateRandomDecimal(f.precision, f.scale, 50)
            }
            is CompoundField -> field2DefaultGenerator(f.fld)
            is AggregateField -> field2DefaultGenerator(f.fld)
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


    fun getDefaultValueGeneratorsForOrc(fields: List<Field>, vararg customizedGenerators: Pair<String, () -> Any>): Map<String, () -> Any> {
        val generators = getDefaultGenerators(fields)
        val largeIntFields = getNamesOfLargeIntFields(fields).toSet()
        val customizedGeneratorsMap = customizedGenerators.toMap();
        return generators.map { (name, gen) ->
            if (largeIntFields.contains(name)) {
                name to RandUtil.generateRandomBigInt(50)
            } else {
                name to gen
            }
        }.map { (name, gen) ->
            if (customizedGeneratorsMap.containsKey(name)) {
                name to customizedGeneratorsMap.getValue(name)
            } else {
                name to gen
            }
        }.toMap()
    }

    fun getDefaultKeyGeneratorsForOrc(fields: List<Field>, vararg customizedGenerators: Pair<String, () -> Any>): Map<String, () -> Any> {
        val generators = getDefaultValueGeneratorsForOrc(fields)
        val customizedGeneratorsMap= customizedGenerators.toMap()
        return generators.map { (key, gen) ->
            if (customizedGeneratorsMap.containsKey(key)) {
                key to customizedGeneratorsMap.getValue(key)
            } else {
                key to RandUtil.getFiniteSetGenerator(20, gen)
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
                        val date = generator() as Date
                        (cv as LongColumnVector).vector[idx()] = date.time / 86400000
                    }
                TYPE_DATETIME ->
                    return {
                        (cv as TimestampColumnVector).set(idx(), generator() as Timestamp)
                    }

            }
            is CharField, is VarCharField ->
                return {
                    val i = idx()
                    val bytes = String(generator() as ByteArray).toByteArray()
                    (cv as BytesColumnVector).setVal(i, bytes)
                }
            is DecimalField ->
                return {
                    (cv as DecimalColumnVector).vector[idx()] = HiveDecimalWritable(
                            HiveDecimal.create(generator() as BigDecimal))
                }
            is DecimalV2Field ->
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
            is TrivialCompoundField -> return generateSetOrcCell(cv, f.fld, generator, chunkMaxSize)
            is AggregateField -> return generateSetOrcCell(cv, f.fld, generator, chunkMaxSize)
        }
    }

    fun generateAppendChunk(keyFields: List<SimpleField>, valueFields: List<Field>, chunkMaxSize: Int, vararg customizedGenerators: Pair<String, () -> Any>): (Int) -> VectorizedRowBatch {
        val fields = keyFields + valueFields;
        val schema = createOrcSchemaFromFields(fields)
        val rowBatch = schema.createRowBatch(chunkMaxSize)
        val keyGenerators = getDefaultKeyGeneratorsForOrc(keyFields, *customizedGenerators)
        val valueGenerators = getDefaultValueGeneratorsForOrc(valueFields, *customizedGenerators)
        val generators = keyGenerators + valueGenerators
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

    fun createOrcBrokerLoadSql(db: String, table: Table, hdfsPath: String): String {
        return Util.renderTemplate("broker_load.sql.template",
                "db" to db,
                "table" to table.tableName,
                "labelId" to System.currentTimeMillis().toString(),
                "hdfsPath" to hdfsPath,
                "format" to "orc",
                "columnList" to (table.keyFields() + table.valueFields(emptySet())).map { it.name })
    }

    fun createOrcFile(path: String, keyFields: List<SimpleField>, valueFields: List<Field>, rowsNum: Int, maxChunkSize: Int,
                      vararg customizedGenerators: Pair<String, () -> Any>) {
        val conf = Configuration()
        val dfsPath = org.apache.hadoop.fs.Path(path)
        val file = File(path)
        val schema = createOrcSchemaFromFields(keyFields + valueFields);
        if (Files.exists(file.toPath())) {
            file.delete()
        }
        val writer = OrcFile.createWriter(dfsPath, OrcFile.writerOptions(conf).setSchema(schema))
        val appendChunk = generateAppendChunk(keyFields, valueFields, maxChunkSize, *customizedGenerators)
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
        val timestampFmt = SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val dateFmt = SimpleDateFormat("yyyy-MM-dd")
        return { i ->
            val ii = if (vector.isRepeating) {
                0
            } else {
                i
            }
            when (desc.category) {
                TypeDescription.Category.STRING,
                TypeDescription.Category.VARCHAR,
                TypeDescription.Category.CHAR,
                TypeDescription.Category.BINARY -> {
                    val bytesVector = vector as BytesColumnVector
                    String(bytesVector.vector[ii], bytesVector.start[ii], bytesVector.length[ii], Charsets.UTF_8)
                }
                TypeDescription.Category.BOOLEAN -> {
                    val longVector = vector as LongColumnVector
                    if (longVector.vector[ii] == 1L) {
                        "TRUE"
                    } else {
                        "FALSE"
                    }
                }

                TypeDescription.Category.BYTE,
                TypeDescription.Category.SHORT,
                TypeDescription.Category.INT,
                TypeDescription.Category.LONG -> {
                    val longVector = vector as LongColumnVector
                    longVector.vector[ii].toString()
                }
                TypeDescription.Category.FLOAT,
                TypeDescription.Category.DOUBLE -> {
                    val byteVector = vector as DoubleColumnVector
                    byteVector.vector[ii].toString()
                }
                TypeDescription.Category.DATE -> {
                    val dateVector = vector as LongColumnVector
                    val date = Date(dateVector.vector[ii] * 86400000)
                    //date.time = dateVector.vector[i]
                    dateFmt.format(date)
                }
                TypeDescription.Category.TIMESTAMP -> {
                    val datetimeVector = vector as TimestampColumnVector
                    timestampFmt.format(datetimeVector.getTime(ii))
                }
                TypeDescription.Category.DECIMAL -> {
                    val decimalVector = vector as DecimalColumnVector
                    decimalVector.vector[ii].hiveDecimal.toFormatString(desc.scale)
                }
                TypeDescription.Category.LIST -> TODO()
                TypeDescription.Category.MAP -> TODO()
                TypeDescription.Category.STRUCT -> TODO()
                TypeDescription.Category.UNION -> TODO()
                else -> TODO()
            }
        }
    }

    fun readOrcFile(path: String, vararg fields: String) {
        val conf = Configuration()
        val dfsPath = org.apache.hadoop.fs.Path(path)
        val reader = OrcFile.createReader(dfsPath, OrcFile.ReaderOptions(conf))
        println(reader.schema)
        val rowBatch = reader.schema.createRowBatch()
        val allFields = reader.schema.fieldNames
        val fieldFilter = if (fields.isEmpty()) {
            allFields.filterNotNull().toSet()
        } else {
            fields.toSet()
        }
        val fieldIndices = (0 until allFields.size).filter { i -> fieldFilter.contains(allFields[i]) }
        val columnFormatterGenerators = Array(reader.schema.fieldNames.size) {
            { vector: ColumnVector, desc: TypeDescription -> nthItemOfColumnWithNullCheck(vector, desc) }
        }
        val columnFormatters = Array(reader.schema.fieldNames.size) {
            { _: Int -> "" }
        }
        val rows = reader.rows()
        while (rows.nextBatch(rowBatch)) {
            for (c: Int in fieldIndices) {
                columnFormatters[c] = columnFormatterGenerators[c](rowBatch.cols[c], reader.schema.findSubtype(c + 1))
            }
            for (i: Int in 0 until rowBatch.size) {
                for (c: Int in fieldIndices) {
                    print("${columnFormatters[c](i.toInt())},\t")
                }
                println()
            }
        }
        rows.close()
    }

    fun orcToCVSFile(orcPath: String, csvPath: String, vararg fields: String) {
        Util.enclosedOutputStream(File(csvPath)) {
            orcToCVSOutputStream(orcPath, it, *fields)
        }
    }

    fun orcToCVS(orcPath: String, vararg fields: String) {
        orcToCVSOutputStream(orcPath, System.out, *fields)
    }

    fun orcToList(orcPath: String, vararg fields: String): List<List<String>> {
        val conf = Configuration()
        val dfsPath = org.apache.hadoop.fs.Path(orcPath)
        val reader = OrcFile.createReader(dfsPath, OrcFile.ReaderOptions(conf))
        //println(reader.schema)
        val rowBatch = reader.schema.createRowBatch()
        val allFields = reader.schema.fieldNames
        val fieldFilter = if (fields.isEmpty()) {
            allFields.filterNotNull().toSet()
        } else {
            fields.toSet()
        }
        val fieldIndices = (0 until allFields.size).filter { i -> fieldFilter.contains(allFields[i]) }
        val columnFormatterGenerators = Array(reader.schema.fieldNames.size) {
            { vector: ColumnVector, desc: TypeDescription -> nthItemOfColumnWithNullCheck(vector, desc) }
        }
        val columnFormatters = Array(reader.schema.fieldNames.size) {
            { _: Int -> "" }
        }
        val rows = reader.rows()
        val tuples = mutableListOf<List<String>>()
        while (rows.nextBatch(rowBatch)) {
            for (c: Int in fieldIndices) {
                columnFormatters[c] = columnFormatterGenerators[c](rowBatch.cols[c], reader.schema.findSubtype(c + 1))
            }
            for (i: Int in 0 until rowBatch.size) {
                val tuple = mutableListOf<String>()
                for (c: Int in fieldIndices) {
                    tuple.add(columnFormatters[fieldIndices[c]](i))
                }
                tuples.add(tuple)
            }
        }
        rows.close()
        return tuples
    }

    fun orcToCVSOutputStream(orcPath: String, csvOut: PrintStream, vararg fields: String) {
        val conf = Configuration()
        val dfsPath = org.apache.hadoop.fs.Path(orcPath)
        val reader = OrcFile.createReader(dfsPath, OrcFile.ReaderOptions(conf))
        //println(reader.schema)
        val rowBatch = reader.schema.createRowBatch()
        val allFields = reader.schema.fieldNames
        val fieldFilter = if (fields.isEmpty()) {
            allFields.filterNotNull().toSet()
        } else {
            fields.toSet()
        }
        val fieldIndices = (0 until allFields.size).filter { i -> fieldFilter.contains(allFields[i]) }
        val columnFormatterGenerators = Array(reader.schema.fieldNames.size) {
            { vector: ColumnVector, desc: TypeDescription -> nthItemOfColumnWithNullCheck(vector, desc) }
        }
        val columnFormatters = Array(reader.schema.fieldNames.size) {
            { _: Int -> "" }
        }
        val rows = reader.rows()
        while (rows.nextBatch(rowBatch)) {
            for (c: Int in fieldIndices) {
                columnFormatters[c] = columnFormatterGenerators[c](rowBatch.cols[c], reader.schema.findSubtype(c + 1))
            }
            for (i: Int in 0 until rowBatch.size) {
                csvOut.print("${columnFormatters[fieldIndices.first()](i.toInt())}")
                for (c: Int in fieldIndices.drop(1)) {
                    csvOut.print(",${columnFormatters[c](i.toInt())}")
                }
                csvOut.println()
            }
        }
        rows.close()
    }
}