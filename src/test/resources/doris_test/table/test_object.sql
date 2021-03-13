DROP TABLE IF EXISTS `test_object`;

CREATE TABLE IF NOT EXISTS `test_object` (
  `v1` int(11) NULL,
  `v2` int(11) NULL,
  `v3` int(11) NULL,
  `v4` int(11) NULL,
  `b1` bitmap BITMAP_UNION NULL,
  `b2` bitmap BITMAP_UNION NULL,
  `b3` bitmap BITMAP_UNION NULL,
  `b4` bitmap BITMAP_UNION NULL,
  `h1` hll hll_union NULL,
  `h2` hll hll_union NULL,
  `h3` hll hll_union NULL,
  `h4` hll hll_union NULL
) ENGINE=OLAP
AGGREGATE KEY(`v1`, `v2`, `v3`, `v4`)
DISTRIBUTED BY HASH(`v1`) BUCKETS 10
PROPERTIES (
 "replication_num" = "1"
);
