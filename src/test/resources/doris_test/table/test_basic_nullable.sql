-- need to add date datetime decimal type
DROP TABLE IF EXISTS `test_basic_nullable`;

CREATE TABLE IF NOT EXISTS `test_basic_nullable` (
  `id_int` int(11) NULL,
  `id_tinyint` tinyint NULL,
  `id_smallint` smallint NULL,
  `id_bigint` bigint NULL,
  `id_largeint` largeint NULL,
  `id_float` float NULL,
  `id_double` double NULL,
  `id_char` char(10) NULL,
  `id_varchar` varchar(100) NULL
) ENGINE=OLAP
DUPLICATE KEY(`id_int`)
DISTRIBUTED BY HASH(`id_int`) BUCKETS 10
PROPERTIES (
 "replication_num" = "1"
);
