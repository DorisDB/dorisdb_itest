insert into test_bitmap_index (id_tinyint, id_smallint, id_int, id_bigint, id_largeint, id_float, id_double, id_char, id_varchar, id_date, id_datetime) values
      (null, null, null, null, null, null, null, null, null, null, null),
      (1, 10, 100, 1000, 10000, 100000.1, 1000000.1, 'kks_char1', 'kks-varchar1', '2020-07-01', '2020-07-01 18:00:00'),
      (2, 20, 200, 2000, 20000, 200000.1, 2000000.1, 'kks_char2', 'kks_varchar2', '2020-07-02', '2020-07-02 18:00:00'),
      (3, 30, 300, 3000, 30000, 300000.1, 3000000.1, 'kks_char3', 'kks_varchar3', '2020-07-03', '2020-07-03 18:00:00'),
      (4, 40, 400, 4000, 40000, 400000.1, 4000000.1, 'kks_char4', 'kks_varchar4', '2020-07-04', '2020-07-04 18:00:00'),
      (5, 50, 500, 6000, 50000, 500000.1, 5000000.1,  null,       'kks_varchar5', '2020-07-05', '2020-07-05 18:00:00'),
      (6, 60, 600, 6000, 60000, 600000.1, 6000000.1, 'kks_char6', null,           '2020-07-06', '2020-07-06 18:00:00'),
      (7, 70, 700, 7000, 70000, 700000.1, 7000000.1, 'kks_char7', 'kks_varchar7',  null,        '2020-07-07 18:00:00'),
      (8, 80, 800, 8000, 80000, 800000.1, 8000000.1, 'kks_char8', 'kks_varchar8',  null,        null),
      (null, null, null, null, null, null, null, null, null, null, null);