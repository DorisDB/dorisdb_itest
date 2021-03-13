DROP TABLE IF EXISTS `test_bloom_filter`;

CREATE TABLE IF NOT EXISTS `test_bloom_filter` (
  `id_smallint` smallint,
  `id_int` int(11),
  `id_bigint` bigint,
  `id_largeint` largeint,
  `id_float` float,
  `id_double` double,
  `id_char` char(10),
  `id_varchar` varchar(100),
  `id_date` DATE,
  `id_datetime` DATETIME
) ENGINE=OLAP
DUPLICATE KEY(`id_smallint`)
DISTRIBUTED BY HASH(`id_smallint`) BUCKETS 10
PROPERTIES (
 "replication_num" = "1",
 "bloom_filter_columns"="id_smallint,id_int,id_bigint,id_largeint,id_char,id_varchar,id_date,id_datetime"
);