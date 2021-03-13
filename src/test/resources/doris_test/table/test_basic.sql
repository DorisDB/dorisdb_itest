DROP TABLE IF EXISTS `test_basic`;

CREATE TABLE IF NOT EXISTS `test_basic` (
  `id_int` int(11) NOT NULL,
  `id_tinyint` tinyint NOT NULL,
  `id_smallint` smallint NOT NULL,
  `id_bigint` bigint NOT NULL,
  `id_largeint` largeint NOT NULL,
  `id_float` float NOT NULL,
  `id_double` double NOT NULL,
  `id_char` char(10) NOT NULL,
  `id_varchar` varchar(100) NOT NULL,
  `id_date` date not null,
  `id_datetime` datetime not null,
  `id_decimal` decimal not null
) ENGINE=OLAP
DUPLICATE KEY(`id_int`)
DISTRIBUTED BY HASH(`id_int`) BUCKETS 10
PROPERTIES (
 "replication_num" = "1"
);