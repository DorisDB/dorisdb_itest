-- need to add date datetime decimal type
DROP TABLE IF EXISTS `test_bitmap_index`;

CREATE TABLE IF NOT EXISTS `test_bitmap_index` (
  `id_tinyint` tinyint,
  `id_smallint` smallint,
  `id_int` int(11),
  `id_bigint` bigint,
  `id_largeint` largeint,
  `id_float` float,
  `id_double` double,
  `id_char` char(10),
  `id_varchar` varchar(100),
  `id_date` DATE,
  `id_datetime` DATETIME,
  INDEX idx_1(id_tinyint) using bitmap,
  INDEX idx_2(id_int) using bitmap,
  INDEX idx_3(id_char) using bitmap
) ENGINE=OLAP
DUPLICATE KEY(`id_tinyint`)
DISTRIBUTED BY HASH(`id_tinyint`) BUCKETS 10
PROPERTIES (
 "replication_num" = "1"
);
