DROP TABLE IF EXISTS `metrics_detail`;

CREATE TABLE if NOT EXISTS `metrics_detail` (
  `tags_id` int(11) NULL COMMENT "",
  `timestamp` datetime NULL COMMENT "",
  `value` double SUM NULL COMMENT ""
) ENGINE=OLAP
AGGREGATE KEY(`tags_id`, `timestamp`)
COMMENT "OLAP"
PARTITION BY RANGE(`timestamp`)
(PARTITION p20200704 VALUES [('0000-01-01 00:00:00'), ('2020-07-05 00:00:00')))
DISTRIBUTED BY HASH(`tags_id`) BUCKETS 32
PROPERTIES (
"replication_num" = "1",
"in_memory" = "false",
"storage_format" = "DEFAULT"
);