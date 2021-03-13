DROP TABLE IF EXISTS `test_window`;

CREATE TABLE IF NOT EXISTS `test_window` (
  `id_int` int(11) NOT NULL,
  `id_tinyint` tinyint NOT NULL,
  `id_smallint` smallint NOT NULL,
  `id_bigint` bigint NOT NULL,
  `id_largeint` largeint NOT NULL,
  `id_float` float NOT NULL,
  `id_double` double NOT NULL,
  `id_char` char(10) NOT NULL,
  `id_varchar` varchar(100) NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(`id_int`)
DISTRIBUTED BY HASH(`id_int`) BUCKETS 1
PROPERTIES (
 "replication_num" = "1"
);

