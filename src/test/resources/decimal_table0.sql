DROP TABLE if exists decimal_table0;

CREATE TABLE if not exists decimal_table0
(
  `id` BIGINT NOT NULL,
  `decimal32` DECIMAL(9,3) NOT NULL,
  `decimal64` DECIMAL(18,6) NOT NULL,
  `decimal128` DECIMAL(38,9) NOT NULL,
  `nullable_decimal32` DECIMAL(9,3) NULL,
  `nullable_decimal64` DECIMAL(18,6) NULL,
  `nullable_decimal128` DECIMAL(38,9) NULL
) ENGINE=OLAP
DUPLICATE KEY(`id`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`id`) BUCKETS 1
PROPERTIES(
"replication_num" = "1",
"in_memory" = "false",
"storage_format" = "DEFAULT"
);
