DROP TABLE IF EXISTS `test_window_nullable`;

CREATE TABLE IF NOT EXISTS `test_window_nullable` (
  `id_int` int(11) NULL,
  `id_bigint` bigint NULL,
  `id_varchar` varchar(100) NULL
) ENGINE=OLAP
DUPLICATE KEY(`id_int`)
DISTRIBUTED BY HASH(`id_int`) BUCKETS 1
PROPERTIES (
 "replication_num" = "1"
);

