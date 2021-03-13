DROP TABLE if exists decimal_all_types;

CREATE TABLE if not exists decimal_all_types
(
key0 INT NOT NULL,
key1 INT NOT NULL,
key2 INT NOT NULL,
col_boolean BOOLEAN NOT NULL,
col_tinyint TINYINT NOT NULL,
col_smallint SMALLINT NOT NULL,
col_int INT NOT NULL,
col_bigint BIGINT NOT NULL,
col_largeint LARGEINT NOT NULL,
col_date DATE NOT NULL,
col_timestamp DATETIME NOT NULL,
col_float FLOAT NOT NULL,
col_double DOUBLE NOT NULL,
col_decimalv2 DECIMAL(27,9) NOT NULL,
col_char CHAR NOT NULL,
col_varchar VARCHAR NOT NULL,
col0_decimal_p9s2 DECIMAL32(9, 2) NOT NULL,
col1_decimal_p9s2 DECIMAL32(9, 2) NOT NULL,
col0_decimal_p15s6 DECIMAL64(15, 6) NOT NULL,
col1_decimal_p15s6 DECIMAL64(15, 6) NOT NULL,
col0_decimal_p15s3 DECIMAL64(15, 3) NOT NULL,
col1_decimal_p15s3 DECIMAL64(15, 3) NOT NULL,
col0_decimal_p38s6 DECIMAL128(38, 6) NOT NULL,
col1_decimal_p38s6 DECIMAL128(38, 6) NOT NULL,
col0_decimal_p38s3 DECIMAL128(38, 3) NOT NULL,
col1_decimal_p38s3 DECIMAL128(38, 3) NOT NULL,
col_nullable_boolean BOOLEAN  NULL,
col_nullable_tinyint TINYINT  NULL,
col_nullable_smallint SMALLINT  NULL,
col_nullable_int INT  NULL,
col_nullable_bigint BIGINT  NULL,
col_nullable_largeint LARGEINT  NULL,
col_nullable_date DATE  NULL,
col_nullable_timestamp DATETIME  NULL,
col_nullable_float FLOAT  NULL,
col_nullable_double DOUBLE  NULL,
col_nullable_decimalv2 DECIMAL(27,9)  NULL,
col_nullable_char CHAR  NULL,
col_nullable_varchar VARCHAR  NULL,
col0_nullable_decimal_p9s2 DECIMAL32(9, 2)  NULL,
col1_nullable_decimal_p9s2 DECIMAL32(9, 2)  NULL,
col0_nullable_decimal_p15s6 DECIMAL64(15, 6)  NULL,
col1_nullable_decimal_p15s6 DECIMAL64(15, 6)  NULL,
col0_nullable_decimal_p15s3 DECIMAL64(15, 3)  NULL,
col1_nullable_decimal_p15s3 DECIMAL64(15, 3)  NULL,
col0_nullable_decimal_p38s6 DECIMAL128(38, 6)  NULL,
col1_nullable_decimal_p38s6 DECIMAL128(38, 6)  NULL,
col0_nullable_decimal_p38s3 DECIMAL128(38, 3)  NULL,
col1_nullable_decimal_p38s3 DECIMAL128(38, 3)  NULL
) ENGINE=OLAP
DUPLICATE KEY(`key0`, `key1`, `key2`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`key0`, `key1`, `key2`) BUCKETS 1
PROPERTIES(
"replication_num" = "1",
"in_memory" = "false",
"storage_format" = "DEFAULT"
);