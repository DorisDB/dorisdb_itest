insert into test_sort
 (id_int, id_varchar, id_date, id_datetime, id_decimal, id_boolean, id_int_not_null)
 values
 (1,    "str_1", "2020-07-01", "2020-07-01 12:00:00", 1.123456789, false, 20190501),
 (2,    NULL,    "2020-07-01", "2020-07-01 12:00:00", 1.123456789, false, 20190502),
 (3,    "str_2", NULL,         "2020-07-01 12:00:00", 1.123456789, false, 1),
 (4,    "str_2", "2020-07-02", "2020-07-01 12:00:00", 1.123456789, false, 2),
 (5,    "str_3", "2020-07-02", NULL,                  1.123456789, false, 0),
 (6,    "str_3", "2020-07-02", "2020-07-01 11:00:00", NULL,        false, 20200601),
 (7,    "str_4", "2020-07-03", "2020-07-01 11:00:00", 2.123456789, NULL,  20200601),
 (8,    "str_4", "2020-07-03", "2020-07-01 11:00:00", 2.123456789, true,  20200601),
 (NULL, NULL,    NULL,         NULL,                  NULL,        NULL,  20200601);