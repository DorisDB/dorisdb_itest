package com.grakra.itest

import com.grakra.TestMethodCapture
import com.grakra.util.HiveClient
import com.grakra.schema.*
import com.grakra.util.Util
import com.grakra.schema.*
import org.testng.Assert
import org.testng.annotations.Listeners
import org.testng.annotations.Test

@Listeners(TestMethodCapture::class)
class VectorizedParquetLoadTest : DorisDBRemoteITest() {
    val db = "parquet_db"
    val fields = listOf(
            SimpleField.fixedLength("k1", FixedLengthType.TYPE_DATE),
            SimpleField.fixedLength("k2", FixedLengthType.TYPE_DATETIME),
            SimpleField.char("k3", 20),
            SimpleField.varchar("k4", 20),
            SimpleField.fixedLength("k5", FixedLengthType.TYPE_BOOLEAN),
            SimpleField.fixedLength("k6", FixedLengthType.TYPE_TINYINT),
            SimpleField.fixedLength("k7", FixedLengthType.TYPE_SMALLINT),
            SimpleField.fixedLength("k8", FixedLengthType.TYPE_INT),
            SimpleField.fixedLength("k9", FixedLengthType.TYPE_BIGINT),
            SimpleField.fixedLength("k10", FixedLengthType.TYPE_LARGEINT),
            SimpleField.fixedLength("k11", FixedLengthType.TYPE_FLOAT),
            SimpleField.fixedLength("k12", FixedLengthType.TYPE_DOUBLE),
            SimpleField.decimalv2("k13", 27, 9)
    ).map { CompoundField.nullable(it, 50) }

    val tableNameVectorized = "parquet_test_table_vectorized"
    val tableVectorized = Table(tableNameVectorized, fields, 3)
    val tableNameNonVectorized = "parquet_test_table_non_vectorized"
    val tableNonVectorized = Table(tableNameNonVectorized, fields, 3)

    @Test
    fun create_db_and_table() {
        create_db(db)
        create_table(db, tableVectorized)
        create_table(db, tableNonVectorized)
    }

    fun admin_show() {
        //val a= admin_show_frontend_config("%load%")
        admin_set_vectorized_load_enable(true)
        val a = admin_show_frontend_config("vectorized_load_enable")
        println(a)
        admin_set_vectorized_load_enable(false)
        val b = admin_show_frontend_config("vectorized_load_enable")
        println(b)
    }

    fun broker_load() {
        val hdfsPath = "/rpf/parquet_files/data.parquet"
        admin_set_vectorized_load_enable(true)
        Util.measureCost("vectorized") {
            broker_load(db, tableVectorized, hdfsPath)
        }
        admin_set_vectorized_load_enable(false)
        Util.measureCost("non-vectorized") {
            broker_load(db, tableNonVectorized, hdfsPath)
        }
    }

    fun broker_load_many_files() {
        create_db(db)
        create_table(db, tableVectorized)
        create_table(db, tableNonVectorized)

        val hdfsPath = "/rpf/parquet_files/many_files/*"
        admin_set_vectorized_load_enable(true)
        Util.measureCost("vectorized") {
            broker_load(db, tableVectorized, hdfsPath)
        }
        admin_set_vectorized_load_enable(false)
        Util.measureCost("non-vectorized") {
            broker_load(db, tableNonVectorized, hdfsPath)
        }
        val fpVectorized = fingerprint_murmur_hash3_32(db, "select * from ${tableVectorized.tableName}")
        val fpNonVectorized = fingerprint_murmur_hash3_32(db, "select * from ${tableNonVectorized.tableName}")
        Assert.assertEquals(fpVectorized, fpNonVectorized)
    }

    fun compare_vectorized_and_non_vectorized() {
        val a = fingerprint_murmur_hash3_32(db, "select * from ${tableVectorized.tableName}")
        val b = fingerprint_murmur_hash3_32(db, "select * from ${tableNonVectorized.tableName}")
        Assert.assertEquals(a, b)
    }

    fun loadSql() {
        val loadSql = ParquetUtil.createParquetBrokerLoadSql(db, tableNonVectorized, "/rpf/parquet_files/data.parquet")
        println(loadSql)
    }

    fun compare_nonvectorized_and_vectorized_load(vectorizedSql: String, nonVectorizedSql: String) {
        create_db(db)
        create_table(db, tableVectorized)
        create_table(db, tableNonVectorized)

        admin_set_vectorized_load_enable(true)
        Util.measureCost("vectorized") {
            broker_load(vectorizedSql)
        }
        admin_set_vectorized_load_enable(false)
        Util.measureCost("non-vectorized") {
            broker_load(nonVectorizedSql)
        }
        compare_each_column(db, tableNameVectorized, tableNameNonVectorized, fields.map { it.name })
        val fpVectorized = fingerprint_murmur_hash3_32(db, "select * from ${tableVectorized.tableName}")
        val fpNonVectorized = fingerprint_murmur_hash3_32(db, "select * from ${tableNonVectorized.tableName}")
        Assert.assertEquals(fpVectorized, fpNonVectorized)
    }


    @Test
    fun test_broker_load_1_column_from_path() {
        val vectorizedSql = """
            USE parquet_db;
            LOAD LABEL parquet_db.load_${System.currentTimeMillis()} (
            DATA INFILE("hdfs://172.26.92.141:9002/rpf/parquet_files/1column_from_path/k1=*/*.parquet")
            INTO TABLE `parquet_test_table_vectorized`
            FORMAT AS "parquet"
            (k2, k3, k4, k5, k6, k7, k8, k9, k10, k11, k12, k13)
            COLUMNS FROM PATH AS(k1)
            SET (k1=concat(k1, "-01"))
            )
            WITH BROKER hdfs ("username"="root", "password"="");
        """.trimIndent()

        val nonVectorizedSql = """
            USE parquet_db;
            LOAD LABEL parquet_db.load_${System.currentTimeMillis()} (
            DATA INFILE("hdfs://172.26.92.141:9002/rpf/parquet_files/1column_from_path/k1=*/*.parquet")
            INTO TABLE `parquet_test_table_non_vectorized`
            FORMAT AS "parquet"
            (k2, k3, k4, k5, k6, k7, k8, k9, k10, k11, k12, k13)
            COLUMNS FROM PATH AS (k1)
            SET (k1=concat(k1, "-01"))
            )
            WITH BROKER hdfs ("username"="root", "password"="");
        """.trimIndent()
        compare_nonvectorized_and_vectorized_load(vectorizedSql, nonVectorizedSql)
    }

    @Test
    fun test_broker_load_1_column_from_path_exclude_k13() {
        val vectorizedSql = """
            USE parquet_db;
            LOAD LABEL parquet_db.load_${System.currentTimeMillis()} (
            DATA INFILE("hdfs://172.26.92.141:9002/rpf/parquet_files/1column_from_path/k1=*/*.parquet")
            INTO TABLE `parquet_test_table_vectorized`
            FORMAT AS "parquet"
            (k2, k3, k4, k5, k6, k7, k8, k9, k10, k11, k12)
            COLUMNS FROM PATH AS(k1)
            SET (k1=concat(k1, "-01"))
            )
            WITH BROKER hdfs ("username"="root", "password"="");
        """.trimIndent()

        val nonVectorizedSql = """
            USE parquet_db;
            LOAD LABEL parquet_db.load_${System.currentTimeMillis()} (
            DATA INFILE("hdfs://172.26.92.141:9002/rpf/parquet_files/1column_from_path/k1=*/*.parquet")
            INTO TABLE `parquet_test_table_non_vectorized`
            FORMAT AS "parquet"
            (k2, k3, k4, k5, k6, k7, k8, k9, k10, k11, k12)
            COLUMNS FROM PATH AS (k1)
            SET (k1=concat(k1, "-01"))
            )
            WITH BROKER hdfs ("username"="root", "password"="");
        """.trimIndent()
        compare_nonvectorized_and_vectorized_load(vectorizedSql, nonVectorizedSql)
    }

    @Test
    fun test_broker_load_2_column_from_path() {
        val vectorizedSql = """
            USE parquet_db;
            LOAD LABEL parquet_db.load_${System.currentTimeMillis()} (
            DATA INFILE("hdfs://172.26.92.141:9002/rpf/parquet_files/2column_from_path/k1=*/k6=*/*.parquet")
            INTO TABLE `parquet_test_table_vectorized`
            FORMAT AS "parquet"
            (k3, k4, k5, k7, k8, k9, k10, k11, k12, k13)
            COLUMNS FROM PATH AS (k1, k6)
            SET (
                k1=concat(k1, "-01"),
                k6=k6*k6,
                k2=concat(k1, "-01 12:30:00")
                )
            )
            WITH BROKER hdfs ("username"="root", "password"="");
        """.trimIndent()

        val nonVectorizedSql = """
            USE parquet_db;
            LOAD LABEL parquet_db.load_${System.currentTimeMillis()} (
            DATA INFILE("hdfs://172.26.92.141:9002/rpf/parquet_files/2column_from_path/k1=*/k6=*/*.parquet")
            INTO TABLE `parquet_test_table_non_vectorized`
            FORMAT AS "parquet"
            (k3, k4, k5, k7, k8, k9, k10, k11, k12, k13)
            COLUMNS FROM PATH AS (k1, k6)
            SET (
                k1=concat(k1, "-01"),
                k6=k6*k6,
                k2=concat(k1, "-01 12:30:00")
                )
            )
            WITH BROKER hdfs ("username"="root", "password"="");
        """.trimIndent()
        compare_nonvectorized_and_vectorized_load(vectorizedSql, nonVectorizedSql)
    }

    @Test
    fun test_broker_load_2_column_from_path_exclude_k13() {
        val vectorizedSql = """
            USE parquet_db;
            LOAD LABEL parquet_db.load_${System.currentTimeMillis()} (
            DATA INFILE("hdfs://172.26.92.141:9002/rpf/parquet_files/2column_from_path/k1=*/k6=*/*.parquet")
            INTO TABLE `parquet_test_table_vectorized`
            FORMAT AS "parquet"
            (k3, k4, k5, k7, k8, k9, k10, k11, k12)
            COLUMNS FROM PATH AS (k1, k6)
            SET (
                k1=concat(k1, "-01"),
                k6=k6+1,
                k2=concat(k1, "-01 12:30:00")
                )
            )
            WITH BROKER hdfs ("username"="root", "password"="");
        """.trimIndent()

        val nonVectorizedSql = """
            USE parquet_db;
            LOAD LABEL parquet_db.load_${System.currentTimeMillis()} (
            DATA INFILE("hdfs://172.26.92.141:9002/rpf/parquet_files/2column_from_path/k1=*/k6=*/*.parquet")
            INTO TABLE `parquet_test_table_non_vectorized`
            FORMAT AS "parquet"
            (k3, k4, k5, k7, k8, k9, k10, k11, k12)
            COLUMNS FROM PATH AS (k1, k6)
            SET (
                k1=concat(k1, "-01"),
                k6=k6+1,
                k2=concat(k1, "-01 12:30:00")
                )
            )
            WITH BROKER hdfs ("username"="root", "password"="");
        """.trimIndent()
        compare_nonvectorized_and_vectorized_load(vectorizedSql, nonVectorizedSql)
    }

    @Test
    fun test_broker_load_3_column_from_path() {
        val vectorizedSql = """
            USE parquet_db;
            LOAD LABEL parquet_db.load_${System.currentTimeMillis()} (
            DATA INFILE("hdfs://172.26.92.141:9002/rpf/parquet_files/3column_from_path/k1=*/k2=*/k3=*/*.parquet")
            INTO TABLE `parquet_test_table_vectorized`
            FORMAT AS "parquet"
            (k6, k7, k8, k9, k10, k11, k12, k13)
            COLUMNS FROM PATH AS (k1, k2, k3)
            SET (
                k1=concat(k1, "-01"),
                k2=str_to_date(k2, "%Y%m%d_%H%i%s"),
                k3=left(k1, 4),
                k4=right(k1,2),
                k5=k3
                )
            )
            WITH BROKER hdfs ("username"="root", "password"="");
        """.trimIndent()

        val nonVectorizedSql = """
            USE parquet_db;
            LOAD LABEL parquet_db.load_${System.currentTimeMillis()} (
            DATA INFILE("hdfs://172.26.92.141:9002/rpf/parquet_files/3column_from_path/k1=*/k2=*/k3=*/*.parquet")
            INTO TABLE `parquet_test_table_non_vectorized`
            FORMAT AS "parquet"
            (k6, k7, k8, k9, k10, k11, k12, k13)
            COLUMNS FROM PATH AS (k1, k2, k3)
            SET (
                k1=concat(k1, "-01"),
                k2=str_to_date(k2, "%Y%m%d_%H%i%s"),
                k3=left(k1, 4),
                k4=right(k1,2),
                k5=k3
                )
            )
            WITH BROKER hdfs ("username"="root", "password"="");
        """.trimIndent()
        compare_nonvectorized_and_vectorized_load(vectorizedSql, nonVectorizedSql)
    }

    @Test
    fun test_broker_load_has_empty_files() {
        val vectorizedSql = """
            USE parquet_db;
            LOAD LABEL parquet_db.load_0_${System.currentTimeMillis()} (
            DATA INFILE("hdfs://172.26.92.141:9002/rpf/parquet_files/has_empty_files/*.parquet")
            INTO TABLE `parquet_test_table_vectorized`
            FORMAT AS "parquet"
            (k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11, k12, k13)
            )
            WITH BROKER hdfs ("username"="root", "password"="");
        """.trimIndent()

        val nonVectorizedSql = """
            USE parquet_db;
            LOAD LABEL parquet_db.load_1_${System.currentTimeMillis()} (
            DATA INFILE("hdfs://172.26.92.141:9002/rpf/parquet_files/has_empty_files/*.parquet")
            INTO TABLE `parquet_test_table_non_vectorized`
            FORMAT AS "parquet"
            (k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11, k12, k13)
            )
            WITH BROKER hdfs ("username"="root", "password"="");
        """.trimIndent()
        compare_nonvectorized_and_vectorized_load(vectorizedSql, nonVectorizedSql)
    }


    val onlyDecimalV2Fields = listOf(
            SimpleField.fixedLength("k1", FixedLengthType.TYPE_DATE),
            SimpleField.decimalv2("k13", 27, 9)).map { CompoundField.nullable(it, 50) }
    val onlyDecimalV2TableVectorized = Table("only_decimalv2_table_vectorized", onlyDecimalV2Fields,
            1)
    val onlyDecimalV2TableNonVectorized = Table("only_decimalv2_table_non_vectorized", onlyDecimalV2Fields,
            1)

    @Test
    fun test_only_decimalv3() {
        val vectorizedSql = """
            USE parquet_db;
            LOAD LABEL parquet_db.load_${System.currentTimeMillis()} (
            DATA INFILE("hdfs://172.26.92.141:9002/rpf/parquet_files/data.parquet")
            INTO TABLE `only_decimalv2_table_vectorized`
            FORMAT AS "parquet"
            (k1, k13)
            )
            WITH BROKER hdfs ("username"="root", "password"="")
            PROPERTIES
            (
                "timeout" = "3600",
                "max_filter_ratio" = "0.1"
            );

            ;
        """.trimIndent()

        val nonVectorizedSql = """
            USE parquet_db;
            LOAD LABEL parquet_db.load_${System.currentTimeMillis()} (
            DATA INFILE("hdfs://172.26.92.141:9002/rpf/parquet_files/data.parquet")
            INTO TABLE `only_decimalv2_table_non_vectorized`
            FORMAT AS "parquet"
            (k1, k13)
            )
            WITH BROKER hdfs ("username"="root", "password"="")
            PROPERTIES
            (
                "timeout" = "3600",
                "max_filter_ratio" = "0.1"
            )
            ;
        """.trimIndent()
        create_db(db)
        create_table(db, onlyDecimalV2TableVectorized)
        create_table(db, onlyDecimalV2TableNonVectorized)

        admin_set_vectorized_load_enable(true)
        Util.measureCost("vectorized") {
            broker_load(vectorizedSql)
        }
        admin_set_vectorized_load_enable(false)
        Util.measureCost("non-vectorized") {
            broker_load(nonVectorizedSql)
        }
        compare_each_column(db, onlyDecimalV2TableVectorized.tableName, onlyDecimalV2TableNonVectorized.tableName, listOf("k1", "k13"))
        val fpVectorized = fingerprint_murmur_hash3_32(db, "select * from ${onlyDecimalV2TableNonVectorized.tableName}")
        val fpNonVectorized = fingerprint_murmur_hash3_32(db, "select * from ${onlyDecimalV2TableVectorized.tableName}")
        Assert.assertEquals(fpVectorized, fpNonVectorized)

    }

    @Test
    fun test_only_decimalv3_v2() {
        val vectorizedSql = """
            USE parquet_db;
            LOAD LABEL parquet_db.load_${System.currentTimeMillis()} (
            DATA INFILE("hdfs://172.26.92.141:9002/rpf/parquet_files/data.parquet")
            INTO TABLE `only_decimalv2_table_vectorized`
            FORMAT AS "parquet"
            (k1, k13)
            )
            WITH BROKER hdfs ("username"="root", "password"="")
            ;
        """.trimIndent()

        val nonVectorizedSql = """
            USE parquet_db;
            LOAD LABEL parquet_db.load_${System.currentTimeMillis()} (
            DATA INFILE("hdfs://172.26.92.141:9002/rpf/parquet_files/data.parquet")
            INTO TABLE `only_decimalv2_table_non_vectorized`
            FORMAT AS "parquet"
            (k1, k13)
            )
            WITH BROKER hdfs ("username"="root", "password"="")
            ;
        """.trimIndent()
        create_db(db)
        create_table(db, onlyDecimalV2TableVectorized)
        create_table(db, onlyDecimalV2TableNonVectorized)

        admin_set_vectorized_load_enable(true)
        Util.measureCost("vectorized") {
            broker_load(vectorizedSql)
        }
        admin_set_vectorized_load_enable(false)
        Util.measureCost("non-vectorized") {
            broker_load(nonVectorizedSql)
        }
        compare_each_column(db, onlyDecimalV2TableVectorized.tableName, onlyDecimalV2TableNonVectorized.tableName, listOf("k1", "k13"))
        val fpVectorized = fingerprint_murmur_hash3_32(db, "select * from ${onlyDecimalV2TableNonVectorized.tableName}")
        val fpNonVectorized = fingerprint_murmur_hash3_32(db, "select * from ${onlyDecimalV2TableVectorized.tableName}")
        Assert.assertEquals(fpVectorized, fpNonVectorized)

    }

    val hiveFields = listOf(
            SimpleField.fixedLength("col_date", FixedLengthType.TYPE_DATE),
            SimpleField.fixedLength("col_datetime", FixedLengthType.TYPE_DATETIME),
            SimpleField.char("col_char", 20),
            SimpleField.varchar("col_varchar", 20),
            SimpleField.fixedLength("col_boolean", FixedLengthType.TYPE_BOOLEAN),
            SimpleField.fixedLength("col_tinyint", FixedLengthType.TYPE_TINYINT),
            SimpleField.fixedLength("col_smallint", FixedLengthType.TYPE_SMALLINT),
            SimpleField.fixedLength("col_int", FixedLengthType.TYPE_INT),
            SimpleField.fixedLength("col_bigint", FixedLengthType.TYPE_BIGINT),
            SimpleField.fixedLength("col_float", FixedLengthType.TYPE_FLOAT),
            SimpleField.fixedLength("col_double", FixedLengthType.TYPE_DOUBLE),
            SimpleField.decimalv2("col_decimal_p6s2", 6, 2),
            SimpleField.decimalv2("col_decimal_p14s5", 14, 5),
            SimpleField.decimalv2("col_decimal_p27s9", 27, 9)
    )

    val hiveTableName = "hive_table0"
    val hiveTable = Table(hiveTableName, hiveFields, 1)
    val nullableHiveTable = Table("nullable_$hiveTableName", hiveFields.map { CompoundField.nullable(it, 50) }, 0)

    fun hiveSql() {
        val csvSql = hiveTable.hiveSql("csv", arrayOf(), arrayOf(), arrayOf(), 10)
        val orcSql = hiveTable.hiveSql("orc", arrayOf(), arrayOf(), arrayOf(), 10)
        val parquetSql = hiveTable.hiveSql("parquet", arrayOf(), arrayOf(), arrayOf(), 10)
        println(csvSql)
        println(orcSql)
        println(parquetSql)
        //OrcUtil.createOrcFile("$hiveTableName.orc", hiveTable.keyFields(), hiveTable.valueFields(emptySet()), 8193, 4096)
        //OrcUtil.orcToCVS("$hiveTableName.orc")
        //OrcUtil.orcToCVSFile("$hiveTableName.orc", "$hiveTableName.csv")
        arrayOf(0, 1, 4095, 4096, 4097, 8191, 8192, 8193).forEach { num_rows ->
            OrcUtil.createOrcFile("${hiveTable.tableName}_$num_rows.orc", hiveTable.keyFields(), hiveTable.valueFields(emptySet()), num_rows, 4096)
            OrcUtil.createOrcFile("${nullableHiveTable.tableName}_$num_rows.orc", nullableHiveTable.keyFields(), nullableHiveTable.valueFields(emptySet()), num_rows, 4096)
        }
    }

    fun hiveClientTest() {
        val hiveClient = HiveClient("127.0.0.1:10000/default", "grakra", "")
        hiveClient.q { hive ->
            hive.qv("show tables")
        }

        val fileSizes = arrayOf(0, 1, 4095, 4096, 4097, 8191, 8192, 8193)
        val orcFiles = fileSizes.map { "/hive_table0_orc_files/hive_table0_$it.orc" }
        val nullableOrcFiles = fileSizes.map { "/nullable_hive_table0_orc_files/nullable_hive_table0_$it.orc" }
        val parquetSql = hiveTable.hiveSql("parquet", arrayOf(), arrayOf(), arrayOf(), 10)
        val nullableParquetSql = nullableHiveTable.hiveSql("parquet", arrayOf(), arrayOf(), arrayOf(), 10)
        hiveClient.q { hive ->
            hive.e("DROP TABLE IF EXISTS parquet_${hiveTable.tableName}")
            hive.e("DROP TABLE IF EXISTS parquet_${nullableHiveTable.tableName}")
            hive.e(parquetSql)
            hive.e(nullableParquetSql)
        }
        val orcSql = hiveTable.hiveSql("orc", arrayOf(), arrayOf(), arrayOf(), 10)
        val nullableOrcSql = nullableHiveTable.hiveSql("orc", arrayOf(), arrayOf(), arrayOf(), 10)
        hiveClient.q { hive ->
            orcFiles.forEach { orcFile ->
                val orcTable = "orc_${hiveTable.tableName}"
                val parquetTable = "parquet_${hiveTable.tableName}"
                val loadOrcSql = "LOAD DATA INPATH 'hdfs://$orcFile'  INTO TABLE $orcTable"
                val insertParquetSql = "INSERT INTO $parquetTable select * from $orcTable"
                hive.e("DROP TABLE IF EXISTS $orcTable")
                hive.e(orcSql)
                hive.e(loadOrcSql)
                hive.e(insertParquetSql)
            }

            nullableOrcFiles.forEach { orcFile ->
                val orcTable = "orc_${nullableHiveTable.tableName}"
                val parquetTable = "parquet_${nullableHiveTable.tableName}"
                val loadOrcSql = "LOAD DATA INPATH 'hdfs://$orcFile'  INTO TABLE $orcTable"
                val insertParquetSql = "INSERT INTO $parquetTable select * from $orcTable"
                hive.e("DROP TABLE IF EXISTS $orcTable")
                hive.e(nullableOrcSql)
                hive.e(loadOrcSql)
                hive.e(insertParquetSql)
            }
        }
    }

    val parquetHiveTableFields = listOf(
            SimpleField.fixedLength("k1", FixedLengthType.TYPE_DATE),
            SimpleField.fixedLength("k2", FixedLengthType.TYPE_DATETIME),
            SimpleField.char("k3", 20),
            SimpleField.varchar("k4", 20),
            SimpleField.fixedLength("k5", FixedLengthType.TYPE_BOOLEAN),
            SimpleField.fixedLength("k6", FixedLengthType.TYPE_TINYINT),
            SimpleField.fixedLength("k7", FixedLengthType.TYPE_SMALLINT),
            SimpleField.fixedLength("k8", FixedLengthType.TYPE_INT),
            SimpleField.fixedLength("k9", FixedLengthType.TYPE_BIGINT),
            //SimpleField.fixedLength("k10", FixedLengthType.TYPE_BIGINT),
            SimpleField.fixedLength("k11", FixedLengthType.TYPE_FLOAT),
            SimpleField.fixedLength("k12", FixedLengthType.TYPE_DOUBLE),
            SimpleField.decimalv2("k13", 38, 9)
    ).map { CompoundField.nullable(it, 50) }
    val parquetHiveTable = Table("hive_table2", parquetHiveTableFields, 1)

    @Test
    fun load_parquet_into_hive() {
        val hiveClient = HiveClient("127.0.0.1:10000/default", "grakra", "")
        val parquetFile = "/parquet_files2/data0.parquet"
        val dropHiveTableSql = "DROP TABLE IF EXISTS parquet_${parquetHiveTable.tableName}"
        val createHiveTableSql = parquetHiveTable.hiveSql("parquet", arrayOf(), arrayOf(), arrayOf(), 1)
        val loadParqeutSql = "LOAD DATA INPATH 'hdfs://$parquetFile'  INTO TABLE parquet_${parquetHiveTable.tableName}"
        hiveClient.q { hive ->
            hive.e(dropHiveTableSql)
            hive.e(createHiveTableSql)
            hive.e(loadParqeutSql)
        }
    }

    @Test
    fun create_hive_table() {
        create_db(db)
        create_table(db, hiveTable)
        broker_load(db, hiveTable, "/rpf/parquet_files/test_data/parquet_data/data_*.parquet")
    }
}
