DROP TABLE IF EXISTS `test_aggregate_table`;

CREATE TABLE `test_aggregate_table` (
  `k1` int(11) NULL COMMENT "",
  `k2` int(11) NULL COMMENT "",
  `k3` varchar(20) NULL COMMENT "",
  `b1` bitmap BITMAP_UNION NULL COMMENT "",
  `b2` bitmap BITMAP_UNION NULL COMMENT "",
  `b3` bitmap BITMAP_UNION NULL COMMENT "",
  `h1` hll HLL_UNION NULL COMMENT "",
  `h2` hll HLL_UNION NULL COMMENT "",
  `h3` hll HLL_UNION NULL COMMENT "",
  `p1` percentile percentile_union NULL COMMENT "",
  `p2` percentile percentile_union NULL COMMENT ""
) ENGINE=OLAP
AGGREGATE KEY(`k1`, `k2`, `k3`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`k1`) BUCKETS 10
PROPERTIES (
"replication_num" = "1",
"in_memory" = "false",
"storage_format" = "V2"
);