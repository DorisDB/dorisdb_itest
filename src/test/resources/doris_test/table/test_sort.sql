DROP TABLE IF EXISTS `test_sort`;

CREATE TABLE IF NOT EXISTS `test_sort` (
    `id_int`        int,
    `id_varchar`    varchar(64),
    `id_date`       date,
    `id_datetime`   datetime,
    `id_decimal`    decimal(10, 9),
    `id_boolean`    boolean,
    `id_int_not_null` int NOT NULL
) ENGINE=OLAP
DUPLICATE kEY(`id_int`)
DISTRIBUTED BY HASH(`id_int`) BUCKETS 10
PROPERTIES (
    "replication_num" = "1"
);