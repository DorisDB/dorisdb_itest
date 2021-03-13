DROP TABLE IF EXISTS `test_all_type_nullable2`;

CREATE TABLE IF NOT EXISTS `test_all_type_nullable2` (
  `id_int` int(11) NOT NULL,
  `id_tinyint` tinyint NOT NULL,
  `id_smallint` smallint NOT NULL,
  `id_bigint` bigint NOT NULL,
  `id_largeint` largeint NOT NULL,
  `id_float` float NOT NULL,
  `id_double` double NOT NULL,
  `id_char` char(10) NOT NULL,
  `id_varchar` varchar(100) NOT NULL,
  `id_date` date NOT NULL,
  `id_datetime` datetime NOT NULL,
  `id_decimal` decimal NOT NULL,
  `id_boolean` boolean NOT NULL,
  `nid_int` int(11) NULL,
  `nid_tinyint` tinyint NULL,
  `nid_smallint` smallint NULL,
  `nid_bigint` bigint NULL,
  `nid_largeint` largeint NULL,
  `nid_float` float NULL,
  `nid_double` double NULL,
  `nid_char` char(10) NULL,
  `nid_varchar` varchar(100) NULL,
  `nid_date` date NULL,
  `nid_datetime` datetime NULL,
  `nid_decimal` decimal NULL,
  `nid_boolean` boolean NULL
) ENGINE=OLAP
DUPLICATE KEY(`id_int`)
DISTRIBUTED BY HASH(`id_int`) BUCKETS 10
PROPERTIES (
 "replication_num" = "1"
);
