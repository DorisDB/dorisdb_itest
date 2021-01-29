TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromBooleanToDecimal32p9s0Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "false", 9, 0, "0", "false" },
    {-1, -1, "true", 9, 0, "1", "false" }
    };
    test_cast_all<TYPE_BOOLEAN, TYPE_DECIMAL32>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromBooleanToDecimal32p9s2Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "true", 9, 2, "1", "false" },
    {-1, -1, "false", 9, 2, "0", "false" }
    };
    test_cast_all<TYPE_BOOLEAN, TYPE_DECIMAL32>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromBooleanToDecimal32p9s9Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "false", 9, 9, "0", "false" },
    {-1, -1, "true", 9, 9, "1", "false" }
    };
    test_cast_all<TYPE_BOOLEAN, TYPE_DECIMAL32>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromBooleanToDecimal32p7s4Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "false", 7, 4, "0", "false" },
    {-1, -1, "true", 7, 4, "1", "false" }
    };
    test_cast_all<TYPE_BOOLEAN, TYPE_DECIMAL32>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromBooleanToDecimal64p18s0Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "false", 18, 0, "0", "false" },
    {-1, -1, "true", 18, 0, "1", "false" }
    };
    test_cast_all<TYPE_BOOLEAN, TYPE_DECIMAL64>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromBooleanToDecimal64p18s2Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "true", 18, 2, "1", "false" },
    {-1, -1, "false", 18, 2, "0", "false" }
    };
    test_cast_all<TYPE_BOOLEAN, TYPE_DECIMAL64>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromBooleanToDecimal64p18s9Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "true", 18, 9, "1", "false" },
    {-1, -1, "false", 18, 9, "0", "false" }
    };
    test_cast_all<TYPE_BOOLEAN, TYPE_DECIMAL64>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromBooleanToDecimal64p18s18Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "true", 18, 18, "1", "false" },
    {-1, -1, "false", 18, 18, "0", "false" }
    };
    test_cast_all<TYPE_BOOLEAN, TYPE_DECIMAL64>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromBooleanToDecimal64p15s13Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "true", 15, 13, "1", "false" },
    {-1, -1, "false", 15, 13, "0", "false" }
    };
    test_cast_all<TYPE_BOOLEAN, TYPE_DECIMAL64>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromBooleanToDecimal128p38s0Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "false", 38, 0, "0", "false" },
    {-1, -1, "true", 38, 0, "1", "false" }
    };
    test_cast_all<TYPE_BOOLEAN, TYPE_DECIMAL128>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromBooleanToDecimal128p38s2Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "true", 38, 2, "1", "false" },
    {-1, -1, "false", 38, 2, "0", "false" }
    };
    test_cast_all<TYPE_BOOLEAN, TYPE_DECIMAL128>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromBooleanToDecimal128p38s9Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "false", 38, 9, "0", "false" },
    {-1, -1, "true", 38, 9, "1", "false" }
    };
    test_cast_all<TYPE_BOOLEAN, TYPE_DECIMAL128>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromBooleanToDecimal128p38s13Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "false", 38, 13, "0", "false" },
    {-1, -1, "true", 38, 13, "1", "false" }
    };
    test_cast_all<TYPE_BOOLEAN, TYPE_DECIMAL128>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromBooleanToDecimal128p38s18Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "true", 38, 18, "1", "false" },
    {-1, -1, "false", 38, 18, "0", "false" }
    };
    test_cast_all<TYPE_BOOLEAN, TYPE_DECIMAL128>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromBooleanToDecimal128p35s30Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "true", 35, 30, "1", "false" },
    {-1, -1, "false", 35, 30, "0", "false" }
    };
    test_cast_all<TYPE_BOOLEAN, TYPE_DECIMAL128>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromTinyIntToDecimal32p9s0Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "-77", 9, 0, "-77", "false" },
    {-1, -1, "-6", 9, 0, "-6", "false" },
    {-1, -1, "91", 9, 0, "91", "false" },
    {-1, -1, "-5", 9, 0, "-5", "false" },
    {-1, -1, "107", 9, 0, "107", "false" },
    {-1, -1, "-1", 9, 0, "-1", "false" },
    {-1, -1, "7", 9, 0, "7", "false" },
    {-1, -1, "27", 9, 0, "27", "false" },
    {-1, -1, "54", 9, 0, "54", "false" },
    {-1, -1, "-3", 9, 0, "-3", "false" },
    {-1, -1, "-4", 9, 0, "-4", "false" },
    {-1, -1, "28", 9, 0, "28", "false" },
    {-1, -1, "-87", 9, 0, "-87", "false" },
    {-1, -1, "46", 9, 0, "46", "false" },
    {-1, -1, "12", 9, 0, "12", "false" },
    {-1, -1, "20", 9, 0, "20", "false" },
    {-1, -1, "0", 9, 0, "0", "false" }
    };
    test_cast_all<TYPE_TINYINT, TYPE_DECIMAL32>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromTinyIntToDecimal32p9s2Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "-88", 9, 2, "-88", "false" },
    {-1, -1, "127", 9, 2, "127", "false" },
    {-1, -1, "24", 9, 2, "24", "false" },
    {-1, -1, "2", 9, 2, "2", "false" },
    {-1, -1, "12", 9, 2, "12", "false" },
    {-1, -1, "75", 9, 2, "75", "false" },
    {-1, -1, "-4", 9, 2, "-4", "false" },
    {-1, -1, "-7", 9, 2, "-7", "false" },
    {-1, -1, "-1", 9, 2, "-1", "false" },
    {-1, -1, "-23", 9, 2, "-23", "false" },
    {-1, -1, "-112", 9, 2, "-112", "false" },
    {-1, -1, "39", 9, 2, "39", "false" },
    {-1, -1, "1", 9, 2, "1", "false" },
    {-1, -1, "-128", 9, 2, "-128", "false" },
    {-1, -1, "14", 9, 2, "14", "false" },
    {-1, -1, "-2", 9, 2, "-2", "false" }
    };
    test_cast_all<TYPE_TINYINT, TYPE_DECIMAL32>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromTinyIntToDecimal32p9s9Abnormal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "-39", 9, 9, "", "true" },
    {-1, -1, "9", 9, 9, "", "true" },
    {-1, -1, "-12", 9, 9, "", "true" },
    {-1, -1, "3", 9, 9, "", "true" },
    {-1, -1, "113", 9, 9, "", "true" },
    {-1, -1, "-4", 9, 9, "", "true" },
    {-1, -1, "120", 9, 9, "", "true" },
    {-1, -1, "5", 9, 9, "", "true" },
    {-1, -1, "15", 9, 9, "", "true" },
    {-1, -1, "-29", 9, 9, "", "true" },
    {-1, -1, "62", 9, 9, "", "true" },
    {-1, -1, "-128", 9, 9, "", "true" },
    {-1, -1, "127", 9, 9, "", "true" }
    };
    test_cast_all_fail<TYPE_TINYINT, TYPE_DECIMAL32>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromTinyIntToDecimal32p9s9Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "-2", 9, 9, "-2", "false" },
    {-1, -1, "1", 9, 9, "1", "false" },
    {-1, -1, "-1", 9, 9, "-1", "false" }
    };
    test_cast_all<TYPE_TINYINT, TYPE_DECIMAL32>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromTinyIntToDecimal32p7s4Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "-119", 7, 4, "-119", "false" },
    {-1, -1, "-1", 7, 4, "-1", "false" },
    {-1, -1, "-51", 7, 4, "-51", "false" },
    {-1, -1, "-13", 7, 4, "-13", "false" },
    {-1, -1, "29", 7, 4, "29", "false" },
    {-1, -1, "-128", 7, 4, "-128", "false" },
    {-1, -1, "-8", 7, 4, "-8", "false" },
    {-1, -1, "-126", 7, 4, "-126", "false" },
    {-1, -1, "71", 7, 4, "71", "false" },
    {-1, -1, "-81", 7, 4, "-81", "false" },
    {-1, -1, "3", 7, 4, "3", "false" },
    {-1, -1, "-63", 7, 4, "-63", "false" },
    {-1, -1, "30", 7, 4, "30", "false" },
    {-1, -1, "102", 7, 4, "102", "false" },
    {-1, -1, "-4", 7, 4, "-4", "false" },
    {-1, -1, "51", 7, 4, "51", "false" }
    };
    test_cast_all<TYPE_TINYINT, TYPE_DECIMAL32>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromTinyIntToDecimal64p18s0Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "-3", 18, 0, "-3", "false" },
    {-1, -1, "17", 18, 0, "17", "false" },
    {-1, -1, "-2", 18, 0, "-2", "false" },
    {-1, -1, "-4", 18, 0, "-4", "false" },
    {-1, -1, "127", 18, 0, "127", "false" },
    {-1, -1, "42", 18, 0, "42", "false" },
    {-1, -1, "-35", 18, 0, "-35", "false" },
    {-1, -1, "-1", 18, 0, "-1", "false" },
    {-1, -1, "5", 18, 0, "5", "false" },
    {-1, -1, "-29", 18, 0, "-29", "false" },
    {-1, -1, "1", 18, 0, "1", "false" },
    {-1, -1, "-9", 18, 0, "-9", "false" },
    {-1, -1, "8", 18, 0, "8", "false" },
    {-1, -1, "11", 18, 0, "11", "false" },
    {-1, -1, "-128", 18, 0, "-128", "false" }
    };
    test_cast_all<TYPE_TINYINT, TYPE_DECIMAL64>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromTinyIntToDecimal64p18s2Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "-29", 18, 2, "-29", "false" },
    {-1, -1, "4", 18, 2, "4", "false" },
    {-1, -1, "-16", 18, 2, "-16", "false" },
    {-1, -1, "127", 18, 2, "127", "false" },
    {-1, -1, "1", 18, 2, "1", "false" },
    {-1, -1, "-43", 18, 2, "-43", "false" },
    {-1, -1, "-1", 18, 2, "-1", "false" },
    {-1, -1, "0", 18, 2, "0", "false" },
    {-1, -1, "-4", 18, 2, "-4", "false" },
    {-1, -1, "-113", 18, 2, "-113", "false" },
    {-1, -1, "-6", 18, 2, "-6", "false" },
    {-1, -1, "-21", 18, 2, "-21", "false" },
    {-1, -1, "6", 18, 2, "6", "false" },
    {-1, -1, "105", 18, 2, "105", "false" },
    {-1, -1, "-2", 18, 2, "-2", "false" }
    };
    test_cast_all<TYPE_TINYINT, TYPE_DECIMAL64>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromTinyIntToDecimal64p18s9Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "-8", 18, 9, "-8", "false" },
    {-1, -1, "-31", 18, 9, "-31", "false" },
    {-1, -1, "7", 18, 9, "7", "false" },
    {-1, -1, "76", 18, 9, "76", "false" },
    {-1, -1, "-128", 18, 9, "-128", "false" },
    {-1, -1, "-1", 18, 9, "-1", "false" },
    {-1, -1, "-107", 18, 9, "-107", "false" },
    {-1, -1, "3", 18, 9, "3", "false" },
    {-1, -1, "127", 18, 9, "127", "false" },
    {-1, -1, "48", 18, 9, "48", "false" },
    {-1, -1, "-3", 18, 9, "-3", "false" },
    {-1, -1, "2", 18, 9, "2", "false" },
    {-1, -1, "-18", 18, 9, "-18", "false" },
    {-1, -1, "26", 18, 9, "26", "false" },
    {-1, -1, "15", 18, 9, "15", "false" },
    {-1, -1, "-27", 18, 9, "-27", "false" },
    {-1, -1, "-6", 18, 9, "-6", "false" },
    {-1, -1, "-40", 18, 9, "-40", "false" }
    };
    test_cast_all<TYPE_TINYINT, TYPE_DECIMAL64>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromTinyIntToDecimal64p18s18Abnormal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "-36", 18, 18, "", "true" },
    {-1, -1, "127", 18, 18, "", "true" },
    {-1, -1, "24", 18, 18, "", "true" },
    {-1, -1, "-18", 18, 18, "", "true" },
    {-1, -1, "44", 18, 18, "", "true" },
    {-1, -1, "-24", 18, 18, "", "true" },
    {-1, -1, "10", 18, 18, "", "true" },
    {-1, -1, "-21", 18, 18, "", "true" },
    {-1, -1, "-31", 18, 18, "", "true" }
    };
    test_cast_all_fail<TYPE_TINYINT, TYPE_DECIMAL64>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromTinyIntToDecimal64p18s18Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "3", 18, 18, "3", "false" },
    {-1, -1, "1", 18, 18, "1", "false" },
    {-1, -1, "-1", 18, 18, "-1", "false" },
    {-1, -1, "0", 18, 18, "0", "false" },
    {-1, -1, "-6", 18, 18, "-6", "false" },
    {-1, -1, "-4", 18, 18, "-4", "false" }
    };
    test_cast_all<TYPE_TINYINT, TYPE_DECIMAL64>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromTinyIntToDecimal64p15s13Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "0", 15, 13, "0", "false" },
    {-1, -1, "1", 15, 13, "1", "false" },
    {-1, -1, "18", 15, 13, "18", "false" },
    {-1, -1, "-76", 15, 13, "-76", "false" },
    {-1, -1, "42", 15, 13, "42", "false" },
    {-1, -1, "7", 15, 13, "7", "false" },
    {-1, -1, "-85", 15, 13, "-85", "false" },
    {-1, -1, "127", 15, 13, "127", "false" },
    {-1, -1, "-1", 15, 13, "-1", "false" },
    {-1, -1, "-15", 15, 13, "-15", "false" },
    {-1, -1, "-10", 15, 13, "-10", "false" },
    {-1, -1, "-3", 15, 13, "-3", "false" },
    {-1, -1, "6", 15, 13, "6", "false" },
    {-1, -1, "-8", 15, 13, "-8", "false" },
    {-1, -1, "-2", 15, 13, "-2", "false" },
    {-1, -1, "-128", 15, 13, "-128", "false" }
    };
    test_cast_all<TYPE_TINYINT, TYPE_DECIMAL64>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromTinyIntToDecimal128p38s0Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "-62", 38, 0, "-62", "false" },
    {-1, -1, "-6", 38, 0, "-6", "false" },
    {-1, -1, "-2", 38, 0, "-2", "false" },
    {-1, -1, "-50", 38, 0, "-50", "false" },
    {-1, -1, "1", 38, 0, "1", "false" },
    {-1, -1, "-4", 38, 0, "-4", "false" },
    {-1, -1, "-59", 38, 0, "-59", "false" },
    {-1, -1, "-23", 38, 0, "-23", "false" },
    {-1, -1, "127", 38, 0, "127", "false" },
    {-1, -1, "-3", 38, 0, "-3", "false" },
    {-1, -1, "99", 38, 0, "99", "false" },
    {-1, -1, "-1", 38, 0, "-1", "false" },
    {-1, -1, "-30", 38, 0, "-30", "false" },
    {-1, -1, "5", 38, 0, "5", "false" },
    {-1, -1, "3", 38, 0, "3", "false" },
    {-1, -1, "22", 38, 0, "22", "false" },
    {-1, -1, "-92", 38, 0, "-92", "false" },
    {-1, -1, "-37", 38, 0, "-37", "false" }
    };
    test_cast_all<TYPE_TINYINT, TYPE_DECIMAL128>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromTinyIntToDecimal128p38s2Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "5", 38, 2, "5", "false" },
    {-1, -1, "1", 38, 2, "1", "false" },
    {-1, -1, "-107", 38, 2, "-107", "false" },
    {-1, -1, "-2", 38, 2, "-2", "false" },
    {-1, -1, "27", 38, 2, "27", "false" },
    {-1, -1, "127", 38, 2, "127", "false" },
    {-1, -1, "-15", 38, 2, "-15", "false" },
    {-1, -1, "7", 38, 2, "7", "false" },
    {-1, -1, "-26", 38, 2, "-26", "false" },
    {-1, -1, "3", 38, 2, "3", "false" },
    {-1, -1, "-61", 38, 2, "-61", "false" },
    {-1, -1, "-59", 38, 2, "-59", "false" },
    {-1, -1, "-73", 38, 2, "-73", "false" },
    {-1, -1, "-27", 38, 2, "-27", "false" },
    {-1, -1, "11", 38, 2, "11", "false" },
    {-1, -1, "-8", 38, 2, "-8", "false" }
    };
    test_cast_all<TYPE_TINYINT, TYPE_DECIMAL128>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromTinyIntToDecimal128p38s9Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "-55", 38, 9, "-55", "false" },
    {-1, -1, "41", 38, 9, "41", "false" },
    {-1, -1, "127", 38, 9, "127", "false" },
    {-1, -1, "-17", 38, 9, "-17", "false" },
    {-1, -1, "0", 38, 9, "0", "false" },
    {-1, -1, "22", 38, 9, "22", "false" },
    {-1, -1, "-128", 38, 9, "-128", "false" },
    {-1, -1, "6", 38, 9, "6", "false" },
    {-1, -1, "1", 38, 9, "1", "false" },
    {-1, -1, "23", 38, 9, "23", "false" },
    {-1, -1, "-3", 38, 9, "-3", "false" },
    {-1, -1, "-86", 38, 9, "-86", "false" },
    {-1, -1, "-20", 38, 9, "-20", "false" },
    {-1, -1, "-5", 38, 9, "-5", "false" },
    {-1, -1, "-25", 38, 9, "-25", "false" },
    {-1, -1, "57", 38, 9, "57", "false" },
    {-1, -1, "-1", 38, 9, "-1", "false" },
    {-1, -1, "55", 38, 9, "55", "false" }
    };
    test_cast_all<TYPE_TINYINT, TYPE_DECIMAL128>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromTinyIntToDecimal128p38s13Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "-9", 38, 13, "-9", "false" },
    {-1, -1, "-128", 38, 13, "-128", "false" },
    {-1, -1, "0", 38, 13, "0", "false" },
    {-1, -1, "-72", 38, 13, "-72", "false" },
    {-1, -1, "-53", 38, 13, "-53", "false" },
    {-1, -1, "7", 38, 13, "7", "false" },
    {-1, -1, "-7", 38, 13, "-7", "false" },
    {-1, -1, "36", 38, 13, "36", "false" },
    {-1, -1, "2", 38, 13, "2", "false" },
    {-1, -1, "1", 38, 13, "1", "false" },
    {-1, -1, "-82", 38, 13, "-82", "false" },
    {-1, -1, "-10", 38, 13, "-10", "false" },
    {-1, -1, "-50", 38, 13, "-50", "false" },
    {-1, -1, "-11", 38, 13, "-11", "false" },
    {-1, -1, "-1", 38, 13, "-1", "false" },
    {-1, -1, "-57", 38, 13, "-57", "false" }
    };
    test_cast_all<TYPE_TINYINT, TYPE_DECIMAL128>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromTinyIntToDecimal128p38s18Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "127", 38, 18, "127", "false" },
    {-1, -1, "-2", 38, 18, "-2", "false" },
    {-1, -1, "1", 38, 18, "1", "false" },
    {-1, -1, "17", 38, 18, "17", "false" },
    {-1, -1, "93", 38, 18, "93", "false" },
    {-1, -1, "-14", 38, 18, "-14", "false" },
    {-1, -1, "-1", 38, 18, "-1", "false" },
    {-1, -1, "-128", 38, 18, "-128", "false" },
    {-1, -1, "22", 38, 18, "22", "false" },
    {-1, -1, "-3", 38, 18, "-3", "false" },
    {-1, -1, "-69", 38, 18, "-69", "false" },
    {-1, -1, "-6", 38, 18, "-6", "false" },
    {-1, -1, "-9", 38, 18, "-9", "false" },
    {-1, -1, "4", 38, 18, "4", "false" }
    };
    test_cast_all<TYPE_TINYINT, TYPE_DECIMAL128>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromTinyIntToDecimal128p35s30Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "0", 35, 30, "0", "false" },
    {-1, -1, "94", 35, 30, "94", "false" },
    {-1, -1, "-2", 35, 30, "-2", "false" },
    {-1, -1, "127", 35, 30, "127", "false" },
    {-1, -1, "-64", 35, 30, "-64", "false" },
    {-1, -1, "-31", 35, 30, "-31", "false" },
    {-1, -1, "-13", 35, 30, "-13", "false" },
    {-1, -1, "14", 35, 30, "14", "false" },
    {-1, -1, "29", 35, 30, "29", "false" },
    {-1, -1, "-38", 35, 30, "-38", "false" },
    {-1, -1, "-1", 35, 30, "-1", "false" },
    {-1, -1, "46", 35, 30, "46", "false" },
    {-1, -1, "-128", 35, 30, "-128", "false" },
    {-1, -1, "-4", 35, 30, "-4", "false" },
    {-1, -1, "-49", 35, 30, "-49", "false" }
    };
    test_cast_all<TYPE_TINYINT, TYPE_DECIMAL128>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromSmallIntToDecimal32p9s0Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "110", 9, 0, "110", "false" },
    {-1, -1, "6", 9, 0, "6", "false" },
    {-1, -1, "-5713", 9, 0, "-5713", "false" },
    {-1, -1, "-1", 9, 0, "-1", "false" },
    {-1, -1, "566", 9, 0, "566", "false" },
    {-1, -1, "-7324", 9, 0, "-7324", "false" },
    {-1, -1, "-32768", 9, 0, "-32768", "false" },
    {-1, -1, "7911", 9, 0, "7911", "false" },
    {-1, -1, "-14611", 9, 0, "-14611", "false" },
    {-1, -1, "14", 9, 0, "14", "false" },
    {-1, -1, "21068", 9, 0, "21068", "false" },
    {-1, -1, "-783", 9, 0, "-783", "false" },
    {-1, -1, "7279", 9, 0, "7279", "false" },
    {-1, -1, "-16", 9, 0, "-16", "false" },
    {-1, -1, "-27", 9, 0, "-27", "false" },
    {-1, -1, "2", 9, 0, "2", "false" },
    {-1, -1, "-11119", 9, 0, "-11119", "false" },
    {-1, -1, "-5", 9, 0, "-5", "false" },
    {-1, -1, "-3078", 9, 0, "-3078", "false" }
    };
    test_cast_all<TYPE_SMALLINT, TYPE_DECIMAL32>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromSmallIntToDecimal32p9s2Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "-847", 9, 2, "-847", "false" },
    {-1, -1, "-342", 9, 2, "-342", "false" },
    {-1, -1, "121", 9, 2, "121", "false" },
    {-1, -1, "-28", 9, 2, "-28", "false" },
    {-1, -1, "5898", 9, 2, "5898", "false" },
    {-1, -1, "3", 9, 2, "3", "false" },
    {-1, -1, "-7451", 9, 2, "-7451", "false" },
    {-1, -1, "-544", 9, 2, "-544", "false" },
    {-1, -1, "687", 9, 2, "687", "false" },
    {-1, -1, "30", 9, 2, "30", "false" },
    {-1, -1, "8045", 9, 2, "8045", "false" },
    {-1, -1, "-190", 9, 2, "-190", "false" },
    {-1, -1, "-6574", 9, 2, "-6574", "false" },
    {-1, -1, "-22447", 9, 2, "-22447", "false" },
    {-1, -1, "-2", 9, 2, "-2", "false" },
    {-1, -1, "32767", 9, 2, "32767", "false" },
    {-1, -1, "-425", 9, 2, "-425", "false" },
    {-1, -1, "-2107", 9, 2, "-2107", "false" },
    {-1, -1, "-89", 9, 2, "-89", "false" },
    {-1, -1, "24415", 9, 2, "24415", "false" }
    };
    test_cast_all<TYPE_SMALLINT, TYPE_DECIMAL32>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromSmallIntToDecimal32p9s9Abnormal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "236", 9, 9, "", "true" },
    {-1, -1, "-56", 9, 9, "", "true" },
    {-1, -1, "-329", 9, 9, "", "true" },
    {-1, -1, "5437", 9, 9, "", "true" },
    {-1, -1, "153", 9, 9, "", "true" },
    {-1, -1, "33", 9, 9, "", "true" },
    {-1, -1, "224", 9, 9, "", "true" },
    {-1, -1, "30758", 9, 9, "", "true" },
    {-1, -1, "993", 9, 9, "", "true" },
    {-1, -1, "-15277", 9, 9, "", "true" },
    {-1, -1, "4", 9, 9, "", "true" },
    {-1, -1, "-119", 9, 9, "", "true" },
    {-1, -1, "3", 9, 9, "", "true" },
    {-1, -1, "268", 9, 9, "", "true" },
    {-1, -1, "12971", 9, 9, "", "true" },
    {-1, -1, "3866", 9, 9, "", "true" },
    {-1, -1, "-30", 9, 9, "", "true" },
    {-1, -1, "-10", 9, 9, "", "true" }
    };
    test_cast_all_fail<TYPE_SMALLINT, TYPE_DECIMAL32>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromSmallIntToDecimal32p9s9Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "-2", 9, 9, "-2", "false" }
    };
    test_cast_all<TYPE_SMALLINT, TYPE_DECIMAL32>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromSmallIntToDecimal32p7s4Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "2", 7, 4, "2", "false" },
    {-1, -1, "-167", 7, 4, "-167", "false" },
    {-1, -1, "72", 7, 4, "72", "false" },
    {-1, -1, "-136", 7, 4, "-136", "false" },
    {-1, -1, "-7", 7, 4, "-7", "false" },
    {-1, -1, "-7564", 7, 4, "-7564", "false" },
    {-1, -1, "0", 7, 4, "0", "false" },
    {-1, -1, "-344", 7, 4, "-344", "false" },
    {-1, -1, "-1", 7, 4, "-1", "false" },
    {-1, -1, "424", 7, 4, "424", "false" },
    {-1, -1, "329", 7, 4, "329", "false" },
    {-1, -1, "3205", 7, 4, "3205", "false" },
    {-1, -1, "-774", 7, 4, "-774", "false" },
    {-1, -1, "-31629", 7, 4, "-31629", "false" },
    {-1, -1, "15", 7, 4, "15", "false" },
    {-1, -1, "-572", 7, 4, "-572", "false" },
    {-1, -1, "16333", 7, 4, "16333", "false" },
    {-1, -1, "-16109", 7, 4, "-16109", "false" }
    };
    test_cast_all<TYPE_SMALLINT, TYPE_DECIMAL32>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromSmallIntToDecimal64p18s0Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "875", 18, 0, "875", "false" },
    {-1, -1, "1", 18, 0, "1", "false" },
    {-1, -1, "-30", 18, 0, "-30", "false" },
    {-1, -1, "2523", 18, 0, "2523", "false" },
    {-1, -1, "-862", 18, 0, "-862", "false" },
    {-1, -1, "-32", 18, 0, "-32", "false" },
    {-1, -1, "-5", 18, 0, "-5", "false" },
    {-1, -1, "-3", 18, 0, "-3", "false" },
    {-1, -1, "-4188", 18, 0, "-4188", "false" },
    {-1, -1, "-12545", 18, 0, "-12545", "false" },
    {-1, -1, "32767", 18, 0, "32767", "false" },
    {-1, -1, "-1", 18, 0, "-1", "false" },
    {-1, -1, "-1931", 18, 0, "-1931", "false" },
    {-1, -1, "75", 18, 0, "75", "false" },
    {-1, -1, "0", 18, 0, "0", "false" },
    {-1, -1, "-440", 18, 0, "-440", "false" },
    {-1, -1, "58", 18, 0, "58", "false" },
    {-1, -1, "397", 18, 0, "397", "false" },
    {-1, -1, "51", 18, 0, "51", "false" }
    };
    test_cast_all<TYPE_SMALLINT, TYPE_DECIMAL64>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromSmallIntToDecimal64p18s2Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "5154", 18, 2, "5154", "false" },
    {-1, -1, "2", 18, 2, "2", "false" },
    {-1, -1, "835", 18, 2, "835", "false" },
    {-1, -1, "-4", 18, 2, "-4", "false" },
    {-1, -1, "-41", 18, 2, "-41", "false" },
    {-1, -1, "32767", 18, 2, "32767", "false" },
    {-1, -1, "-32768", 18, 2, "-32768", "false" },
    {-1, -1, "12027", 18, 2, "12027", "false" },
    {-1, -1, "-3", 18, 2, "-3", "false" },
    {-1, -1, "-2", 18, 2, "-2", "false" },
    {-1, -1, "58", 18, 2, "58", "false" },
    {-1, -1, "-6512", 18, 2, "-6512", "false" },
    {-1, -1, "-2041", 18, 2, "-2041", "false" },
    {-1, -1, "29464", 18, 2, "29464", "false" },
    {-1, -1, "-1906", 18, 2, "-1906", "false" },
    {-1, -1, "-3415", 18, 2, "-3415", "false" },
    {-1, -1, "31458", 18, 2, "31458", "false" },
    {-1, -1, "15421", 18, 2, "15421", "false" },
    {-1, -1, "171", 18, 2, "171", "false" }
    };
    test_cast_all<TYPE_SMALLINT, TYPE_DECIMAL64>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromSmallIntToDecimal64p18s9Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "0", 18, 9, "0", "false" },
    {-1, -1, "7170", 18, 9, "7170", "false" },
    {-1, -1, "-6", 18, 9, "-6", "false" },
    {-1, -1, "-1", 18, 9, "-1", "false" },
    {-1, -1, "1029", 18, 9, "1029", "false" },
    {-1, -1, "217", 18, 9, "217", "false" },
    {-1, -1, "-27", 18, 9, "-27", "false" },
    {-1, -1, "-213", 18, 9, "-213", "false" },
    {-1, -1, "14539", 18, 9, "14539", "false" },
    {-1, -1, "-87", 18, 9, "-87", "false" },
    {-1, -1, "32767", 18, 9, "32767", "false" },
    {-1, -1, "-7231", 18, 9, "-7231", "false" },
    {-1, -1, "1187", 18, 9, "1187", "false" },
    {-1, -1, "1", 18, 9, "1", "false" },
    {-1, -1, "-5444", 18, 9, "-5444", "false" },
    {-1, -1, "-2773", 18, 9, "-2773", "false" },
    {-1, -1, "6", 18, 9, "6", "false" },
    {-1, -1, "-32768", 18, 9, "-32768", "false" },
    {-1, -1, "-188", 18, 9, "-188", "false" },
    {-1, -1, "16734", 18, 9, "16734", "false" }
    };
    test_cast_all<TYPE_SMALLINT, TYPE_DECIMAL64>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromSmallIntToDecimal64p18s18Abnormal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "1613", 18, 18, "", "true" },
    {-1, -1, "328", 18, 18, "", "true" },
    {-1, -1, "3461", 18, 18, "", "true" },
    {-1, -1, "-10", 18, 18, "", "true" },
    {-1, -1, "55", 18, 18, "", "true" },
    {-1, -1, "-18867", 18, 18, "", "true" },
    {-1, -1, "-1147", 18, 18, "", "true" },
    {-1, -1, "-21", 18, 18, "", "true" },
    {-1, -1, "-16107", 18, 18, "", "true" },
    {-1, -1, "-24646", 18, 18, "", "true" },
    {-1, -1, "-15366", 18, 18, "", "true" },
    {-1, -1, "-2711", 18, 18, "", "true" }
    };
    test_cast_all_fail<TYPE_SMALLINT, TYPE_DECIMAL64>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromSmallIntToDecimal64p18s18Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "-1", 18, 18, "-1", "false" },
    {-1, -1, "1", 18, 18, "1", "false" },
    {-1, -1, "0", 18, 18, "0", "false" },
    {-1, -1, "3", 18, 18, "3", "false" },
    {-1, -1, "-9", 18, 18, "-9", "false" },
    {-1, -1, "6", 18, 18, "6", "false" }
    };
    test_cast_all<TYPE_SMALLINT, TYPE_DECIMAL64>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromSmallIntToDecimal64p15s13Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "3", 15, 13, "3", "false" },
    {-1, -1, "0", 15, 13, "0", "false" },
    {-1, -1, "930", 15, 13, "930", "false" },
    {-1, -1, "88", 15, 13, "88", "false" },
    {-1, -1, "224", 15, 13, "224", "false" },
    {-1, -1, "15", 15, 13, "15", "false" },
    {-1, -1, "-32768", 15, 13, "-32768", "false" },
    {-1, -1, "-122", 15, 13, "-122", "false" },
    {-1, -1, "-46", 15, 13, "-46", "false" },
    {-1, -1, "4012", 15, 13, "4012", "false" },
    {-1, -1, "454", 15, 13, "454", "false" },
    {-1, -1, "-13496", 15, 13, "-13496", "false" },
    {-1, -1, "-1484", 15, 13, "-1484", "false" },
    {-1, -1, "-6513", 15, 13, "-6513", "false" },
    {-1, -1, "-4", 15, 13, "-4", "false" },
    {-1, -1, "-16319", 15, 13, "-16319", "false" },
    {-1, -1, "-2595", 15, 13, "-2595", "false" },
    {-1, -1, "17830", 15, 13, "17830", "false" },
    {-1, -1, "32767", 15, 13, "32767", "false" },
    {-1, -1, "2", 15, 13, "2", "false" }
    };
    test_cast_all<TYPE_SMALLINT, TYPE_DECIMAL64>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromSmallIntToDecimal128p38s0Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "-597", 38, 0, "-597", "false" },
    {-1, -1, "2031", 38, 0, "2031", "false" },
    {-1, -1, "-13141", 38, 0, "-13141", "false" },
    {-1, -1, "-32768", 38, 0, "-32768", "false" },
    {-1, -1, "125", 38, 0, "125", "false" },
    {-1, -1, "-1386", 38, 0, "-1386", "false" },
    {-1, -1, "0", 38, 0, "0", "false" },
    {-1, -1, "-8200", 38, 0, "-8200", "false" },
    {-1, -1, "-8099", 38, 0, "-8099", "false" },
    {-1, -1, "-7", 38, 0, "-7", "false" },
    {-1, -1, "768", 38, 0, "768", "false" },
    {-1, -1, "-18", 38, 0, "-18", "false" },
    {-1, -1, "-10", 38, 0, "-10", "false" },
    {-1, -1, "523", 38, 0, "523", "false" },
    {-1, -1, "-145", 38, 0, "-145", "false" },
    {-1, -1, "990", 38, 0, "990", "false" },
    {-1, -1, "-5423", 38, 0, "-5423", "false" },
    {-1, -1, "-4119", 38, 0, "-4119", "false" }
    };
    test_cast_all<TYPE_SMALLINT, TYPE_DECIMAL128>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromSmallIntToDecimal128p38s2Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "420", 38, 2, "420", "false" },
    {-1, -1, "5038", 38, 2, "5038", "false" },
    {-1, -1, "-3904", 38, 2, "-3904", "false" },
    {-1, -1, "-7", 38, 2, "-7", "false" },
    {-1, -1, "-1667", 38, 2, "-1667", "false" },
    {-1, -1, "-5", 38, 2, "-5", "false" },
    {-1, -1, "305", 38, 2, "305", "false" },
    {-1, -1, "125", 38, 2, "125", "false" },
    {-1, -1, "-55", 38, 2, "-55", "false" },
    {-1, -1, "6", 38, 2, "6", "false" },
    {-1, -1, "26", 38, 2, "26", "false" },
    {-1, -1, "-16", 38, 2, "-16", "false" },
    {-1, -1, "379", 38, 2, "379", "false" },
    {-1, -1, "-502", 38, 2, "-502", "false" },
    {-1, -1, "-10236", 38, 2, "-10236", "false" },
    {-1, -1, "7", 38, 2, "7", "false" },
    {-1, -1, "-3851", 38, 2, "-3851", "false" },
    {-1, -1, "-32768", 38, 2, "-32768", "false" },
    {-1, -1, "5", 38, 2, "5", "false" },
    {-1, -1, "-61", 38, 2, "-61", "false" }
    };
    test_cast_all<TYPE_SMALLINT, TYPE_DECIMAL128>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromSmallIntToDecimal128p38s9Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "-601", 38, 9, "-601", "false" },
    {-1, -1, "5615", 38, 9, "5615", "false" },
    {-1, -1, "-416", 38, 9, "-416", "false" },
    {-1, -1, "-1199", 38, 9, "-1199", "false" },
    {-1, -1, "-69", 38, 9, "-69", "false" },
    {-1, -1, "0", 38, 9, "0", "false" },
    {-1, -1, "-4", 38, 9, "-4", "false" },
    {-1, -1, "-2", 38, 9, "-2", "false" },
    {-1, -1, "2518", 38, 9, "2518", "false" },
    {-1, -1, "30", 38, 9, "30", "false" },
    {-1, -1, "-68", 38, 9, "-68", "false" },
    {-1, -1, "-8", 38, 9, "-8", "false" },
    {-1, -1, "1241", 38, 9, "1241", "false" },
    {-1, -1, "271", 38, 9, "271", "false" },
    {-1, -1, "-1041", 38, 9, "-1041", "false" },
    {-1, -1, "-3", 38, 9, "-3", "false" },
    {-1, -1, "-244", 38, 9, "-244", "false" },
    {-1, -1, "-1724", 38, 9, "-1724", "false" }
    };
    test_cast_all<TYPE_SMALLINT, TYPE_DECIMAL128>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromSmallIntToDecimal128p38s13Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "-1", 38, 13, "-1", "false" },
    {-1, -1, "-656", 38, 13, "-656", "false" },
    {-1, -1, "-3755", 38, 13, "-3755", "false" },
    {-1, -1, "24", 38, 13, "24", "false" },
    {-1, -1, "-7", 38, 13, "-7", "false" },
    {-1, -1, "5088", 38, 13, "5088", "false" },
    {-1, -1, "15869", 38, 13, "15869", "false" },
    {-1, -1, "-12192", 38, 13, "-12192", "false" },
    {-1, -1, "-32768", 38, 13, "-32768", "false" },
    {-1, -1, "1539", 38, 13, "1539", "false" },
    {-1, -1, "1", 38, 13, "1", "false" },
    {-1, -1, "-10732", 38, 13, "-10732", "false" },
    {-1, -1, "7", 38, 13, "7", "false" },
    {-1, -1, "-87", 38, 13, "-87", "false" },
    {-1, -1, "-15", 38, 13, "-15", "false" },
    {-1, -1, "1262", 38, 13, "1262", "false" },
    {-1, -1, "-28", 38, 13, "-28", "false" },
    {-1, -1, "-31028", 38, 13, "-31028", "false" },
    {-1, -1, "15", 38, 13, "15", "false" },
    {-1, -1, "-1496", 38, 13, "-1496", "false" }
    };
    test_cast_all<TYPE_SMALLINT, TYPE_DECIMAL128>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromSmallIntToDecimal128p38s18Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "0", 38, 18, "0", "false" },
    {-1, -1, "7913", 38, 18, "7913", "false" },
    {-1, -1, "-3", 38, 18, "-3", "false" },
    {-1, -1, "1632", 38, 18, "1632", "false" },
    {-1, -1, "-1824", 38, 18, "-1824", "false" },
    {-1, -1, "301", 38, 18, "301", "false" },
    {-1, -1, "-840", 38, 18, "-840", "false" },
    {-1, -1, "997", 38, 18, "997", "false" },
    {-1, -1, "32767", 38, 18, "32767", "false" },
    {-1, -1, "9", 38, 18, "9", "false" },
    {-1, -1, "1553", 38, 18, "1553", "false" },
    {-1, -1, "-878", 38, 18, "-878", "false" },
    {-1, -1, "6714", 38, 18, "6714", "false" },
    {-1, -1, "-8628", 38, 18, "-8628", "false" },
    {-1, -1, "19", 38, 18, "19", "false" },
    {-1, -1, "188", 38, 18, "188", "false" },
    {-1, -1, "21", 38, 18, "21", "false" },
    {-1, -1, "820", 38, 18, "820", "false" },
    {-1, -1, "-111", 38, 18, "-111", "false" },
    {-1, -1, "-16371", 38, 18, "-16371", "false" }
    };
    test_cast_all<TYPE_SMALLINT, TYPE_DECIMAL128>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromSmallIntToDecimal128p35s30Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "-770", 35, 30, "-770", "false" },
    {-1, -1, "6", 35, 30, "6", "false" },
    {-1, -1, "-32768", 35, 30, "-32768", "false" },
    {-1, -1, "-29", 35, 30, "-29", "false" },
    {-1, -1, "-7", 35, 30, "-7", "false" },
    {-1, -1, "-1708", 35, 30, "-1708", "false" },
    {-1, -1, "-195", 35, 30, "-195", "false" },
    {-1, -1, "-126", 35, 30, "-126", "false" },
    {-1, -1, "1", 35, 30, "1", "false" },
    {-1, -1, "-1222", 35, 30, "-1222", "false" },
    {-1, -1, "-3", 35, 30, "-3", "false" },
    {-1, -1, "4", 35, 30, "4", "false" },
    {-1, -1, "878", 35, 30, "878", "false" },
    {-1, -1, "154", 35, 30, "154", "false" },
    {-1, -1, "12", 35, 30, "12", "false" },
    {-1, -1, "-1", 35, 30, "-1", "false" },
    {-1, -1, "1599", 35, 30, "1599", "false" }
    };
    test_cast_all<TYPE_SMALLINT, TYPE_DECIMAL128>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromIntToDecimal32p9s0Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "67894", 9, 0, "67894", "false" },
    {-1, -1, "-4", 9, 0, "-4", "false" },
    {-1, -1, "719406", 9, 0, "719406", "false" },
    {-1, -1, "0", 9, 0, "0", "false" },
    {-1, -1, "-19983", 9, 0, "-19983", "false" },
    {-1, -1, "246096184", 9, 0, "246096184", "false" },
    {-1, -1, "-60", 9, 0, "-60", "false" },
    {-1, -1, "-166103", 9, 0, "-166103", "false" },
    {-1, -1, "-520842", 9, 0, "-520842", "false" },
    {-1, -1, "126", 9, 0, "126", "false" },
    {-1, -1, "136", 9, 0, "136", "false" },
    {-1, -1, "16842", 9, 0, "16842", "false" },
    {-1, -1, "1594", 9, 0, "1594", "false" },
    {-1, -1, "-7", 9, 0, "-7", "false" },
    {-1, -1, "677", 9, 0, "677", "false" },
    {-1, -1, "-109691", 9, 0, "-109691", "false" },
    {-1, -1, "42568993", 9, 0, "42568993", "false" },
    {-1, -1, "1467", 9, 0, "1467", "false" },
    {-1, -1, "14", 9, 0, "14", "false" }
    };
    test_cast_all<TYPE_INT, TYPE_DECIMAL32>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromIntToDecimal32p9s2Abnormal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "48899437", 9, 2, "", "true" },
    {-1, -1, "-535591307", 9, 2, "", "true" },
    {-1, -1, "-2147483648", 9, 2, "", "true" },
    {-1, -1, "-132725535", 9, 2, "", "true" }
    };
    test_cast_all_fail<TYPE_INT, TYPE_DECIMAL32>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromIntToDecimal32p9s2Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "113827", 9, 2, "113827", "false" },
    {-1, -1, "1494166", 9, 2, "1494166", "false" },
    {-1, -1, "6062483", 9, 2, "6062483", "false" },
    {-1, -1, "-14173761", 9, 2, "-14173761", "false" },
    {-1, -1, "-63", 9, 2, "-63", "false" },
    {-1, -1, "-980091", 9, 2, "-980091", "false" },
    {-1, -1, "25", 9, 2, "25", "false" },
    {-1, -1, "430", 9, 2, "430", "false" },
    {-1, -1, "2", 9, 2, "2", "false" },
    {-1, -1, "249", 9, 2, "249", "false" },
    {-1, -1, "1", 9, 2, "1", "false" },
    {-1, -1, "-4142545", 9, 2, "-4142545", "false" },
    {-1, -1, "17", 9, 2, "17", "false" },
    {-1, -1, "341797", 9, 2, "341797", "false" },
    {-1, -1, "218", 9, 2, "218", "false" },
    {-1, -1, "-193", 9, 2, "-193", "false" }
    };
    test_cast_all<TYPE_INT, TYPE_DECIMAL32>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromIntToDecimal32p9s9Abnormal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "-64611963", 9, 9, "", "true" },
    {-1, -1, "-2147483648", 9, 9, "", "true" },
    {-1, -1, "1301", 9, 9, "", "true" },
    {-1, -1, "-125637", 9, 9, "", "true" },
    {-1, -1, "-266845604", 9, 9, "", "true" },
    {-1, -1, "774964674", 9, 9, "", "true" },
    {-1, -1, "-6", 9, 9, "", "true" },
    {-1, -1, "10808", 9, 9, "", "true" },
    {-1, -1, "549", 9, 9, "", "true" },
    {-1, -1, "3", 9, 9, "", "true" },
    {-1, -1, "378984", 9, 9, "", "true" },
    {-1, -1, "-25", 9, 9, "", "true" },
    {-1, -1, "5581290", 9, 9, "", "true" },
    {-1, -1, "13", 9, 9, "", "true" },
    {-1, -1, "-524", 9, 9, "", "true" },
    {-1, -1, "-66", 9, 9, "", "true" },
    {-1, -1, "41006358", 9, 9, "", "true" },
    {-1, -1, "-1235591", 9, 9, "", "true" }
    };
    test_cast_all_fail<TYPE_INT, TYPE_DECIMAL32>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromIntToDecimal32p9s9Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "0", 9, 9, "0", "false" }
    };
    test_cast_all<TYPE_INT, TYPE_DECIMAL32>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromIntToDecimal32p7s4Abnormal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "164210743", 7, 4, "", "true" },
    {-1, -1, "8152493", 7, 4, "", "true" },
    {-1, -1, "1359132658", 7, 4, "", "true" },
    {-1, -1, "-14940301", 7, 4, "", "true" },
    {-1, -1, "849164474", 7, 4, "", "true" },
    {-1, -1, "20273221", 7, 4, "", "true" },
    {-1, -1, "1966608322", 7, 4, "", "true" },
    {-1, -1, "47966145", 7, 4, "", "true" }
    };
    test_cast_all_fail<TYPE_INT, TYPE_DECIMAL32>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromIntToDecimal32p7s4Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "1", 7, 4, "1", "false" },
    {-1, -1, "5", 7, 4, "5", "false" },
    {-1, -1, "903", 7, 4, "903", "false" },
    {-1, -1, "130858", 7, 4, "130858", "false" },
    {-1, -1, "52227", 7, 4, "52227", "false" },
    {-1, -1, "14", 7, 4, "14", "false" },
    {-1, -1, "-22", 7, 4, "-22", "false" },
    {-1, -1, "-8630", 7, 4, "-8630", "false" },
    {-1, -1, "-2", 7, 4, "-2", "false" },
    {-1, -1, "0", 7, 4, "0", "false" },
    {-1, -1, "109774", 7, 4, "109774", "false" },
    {-1, -1, "-842", 7, 4, "-842", "false" }
    };
    test_cast_all<TYPE_INT, TYPE_DECIMAL32>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromIntToDecimal64p18s0Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "-139224", 18, 0, "-139224", "false" },
    {-1, -1, "-1394346", 18, 0, "-1394346", "false" },
    {-1, -1, "4151", 18, 0, "4151", "false" },
    {-1, -1, "11", 18, 0, "11", "false" },
    {-1, -1, "5593", 18, 0, "5593", "false" },
    {-1, -1, "-417100022", 18, 0, "-417100022", "false" },
    {-1, -1, "1", 18, 0, "1", "false" },
    {-1, -1, "-37", 18, 0, "-37", "false" },
    {-1, -1, "1548378", 18, 0, "1548378", "false" },
    {-1, -1, "53677749", 18, 0, "53677749", "false" },
    {-1, -1, "-2242", 18, 0, "-2242", "false" },
    {-1, -1, "1917703666", 18, 0, "1917703666", "false" },
    {-1, -1, "-23126", 18, 0, "-23126", "false" },
    {-1, -1, "-2147483648", 18, 0, "-2147483648", "false" },
    {-1, -1, "48028", 18, 0, "48028", "false" },
    {-1, -1, "-1", 18, 0, "-1", "false" },
    {-1, -1, "-68", 18, 0, "-68", "false" },
    {-1, -1, "-196773174", 18, 0, "-196773174", "false" },
    {-1, -1, "4129632", 18, 0, "4129632", "false" },
    {-1, -1, "10251", 18, 0, "10251", "false" }
    };
    test_cast_all<TYPE_INT, TYPE_DECIMAL64>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromIntToDecimal64p18s2Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "50107509", 18, 2, "50107509", "false" },
    {-1, -1, "-1430786", 18, 2, "-1430786", "false" },
    {-1, -1, "14657897", 18, 2, "14657897", "false" },
    {-1, -1, "-103265", 18, 2, "-103265", "false" },
    {-1, -1, "520151511", 18, 2, "520151511", "false" },
    {-1, -1, "-1163", 18, 2, "-1163", "false" },
    {-1, -1, "-112", 18, 2, "-112", "false" },
    {-1, -1, "407", 18, 2, "407", "false" },
    {-1, -1, "-9201", 18, 2, "-9201", "false" },
    {-1, -1, "-904514848", 18, 2, "-904514848", "false" },
    {-1, -1, "269", 18, 2, "269", "false" },
    {-1, -1, "-32735", 18, 2, "-32735", "false" },
    {-1, -1, "-10", 18, 2, "-10", "false" },
    {-1, -1, "-352848182", 18, 2, "-352848182", "false" },
    {-1, -1, "-3403", 18, 2, "-3403", "false" },
    {-1, -1, "371693656", 18, 2, "371693656", "false" },
    {-1, -1, "43261856", 18, 2, "43261856", "false" },
    {-1, -1, "-678", 18, 2, "-678", "false" },
    {-1, -1, "-1562", 18, 2, "-1562", "false" },
    {-1, -1, "2572", 18, 2, "2572", "false" }
    };
    test_cast_all<TYPE_INT, TYPE_DECIMAL64>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromIntToDecimal64p18s9Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "-69823", 18, 9, "-69823", "false" },
    {-1, -1, "-90", 18, 9, "-90", "false" },
    {-1, -1, "8260270", 18, 9, "8260270", "false" },
    {-1, -1, "-2", 18, 9, "-2", "false" },
    {-1, -1, "-2147483648", 18, 9, "-2147483648", "false" },
    {-1, -1, "14331969", 18, 9, "14331969", "false" },
    {-1, -1, "96", 18, 9, "96", "false" },
    {-1, -1, "-5", 18, 9, "-5", "false" },
    {-1, -1, "1", 18, 9, "1", "false" },
    {-1, -1, "-1588", 18, 9, "-1588", "false" },
    {-1, -1, "790", 18, 9, "790", "false" },
    {-1, -1, "45", 18, 9, "45", "false" },
    {-1, -1, "-223226690", 18, 9, "-223226690", "false" },
    {-1, -1, "-22007", 18, 9, "-22007", "false" },
    {-1, -1, "-626", 18, 9, "-626", "false" },
    {-1, -1, "954981", 18, 9, "954981", "false" },
    {-1, -1, "376407525", 18, 9, "376407525", "false" },
    {-1, -1, "18913", 18, 9, "18913", "false" },
    {-1, -1, "804088155", 18, 9, "804088155", "false" },
    {-1, -1, "-1903703481", 18, 9, "-1903703481", "false" }
    };
    test_cast_all<TYPE_INT, TYPE_DECIMAL64>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromIntToDecimal64p18s18Abnormal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "66277129", 18, 18, "", "true" },
    {-1, -1, "18", 18, 18, "", "true" },
    {-1, -1, "-28", 18, 18, "", "true" },
    {-1, -1, "-516228", 18, 18, "", "true" },
    {-1, -1, "2415728", 18, 18, "", "true" },
    {-1, -1, "-995418", 18, 18, "", "true" },
    {-1, -1, "-246171", 18, 18, "", "true" },
    {-1, -1, "11284", 18, 18, "", "true" },
    {-1, -1, "-335026", 18, 18, "", "true" },
    {-1, -1, "1153834", 18, 18, "", "true" },
    {-1, -1, "164033948", 18, 18, "", "true" },
    {-1, -1, "181", 18, 18, "", "true" },
    {-1, -1, "108555", 18, 18, "", "true" },
    {-1, -1, "-9361612", 18, 18, "", "true" },
    {-1, -1, "4615416", 18, 18, "", "true" },
    {-1, -1, "-105741", 18, 18, "", "true" },
    {-1, -1, "-127", 18, 18, "", "true" },
    {-1, -1, "-534229029", 18, 18, "", "true" }
    };
    test_cast_all_fail<TYPE_INT, TYPE_DECIMAL64>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromIntToDecimal64p18s18Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "-2", 18, 18, "-2", "false" },
    {-1, -1, "-8", 18, 18, "-8", "false" }
    };
    test_cast_all<TYPE_INT, TYPE_DECIMAL64>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromIntToDecimal64p15s13Abnormal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "2147483647", 15, 13, "", "true" },
    {-1, -1, "-8162749", 15, 13, "", "true" },
    {-1, -1, "156590116", 15, 13, "", "true" },
    {-1, -1, "16507163", 15, 13, "", "true" },
    {-1, -1, "-11193414", 15, 13, "", "true" }
    };
    test_cast_all_fail<TYPE_INT, TYPE_DECIMAL64>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromIntToDecimal64p15s13Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "14530", 15, 13, "14530", "false" },
    {-1, -1, "-1347", 15, 13, "-1347", "false" },
    {-1, -1, "22", 15, 13, "22", "false" },
    {-1, -1, "31561", 15, 13, "31561", "false" },
    {-1, -1, "140511", 15, 13, "140511", "false" },
    {-1, -1, "7117", 15, 13, "7117", "false" },
    {-1, -1, "48730", 15, 13, "48730", "false" },
    {-1, -1, "229116", 15, 13, "229116", "false" },
    {-1, -1, "55985", 15, 13, "55985", "false" },
    {-1, -1, "7", 15, 13, "7", "false" },
    {-1, -1, "186", 15, 13, "186", "false" },
    {-1, -1, "157536", 15, 13, "157536", "false" },
    {-1, -1, "3695", 15, 13, "3695", "false" },
    {-1, -1, "1260", 15, 13, "1260", "false" },
    {-1, -1, "2692", 15, 13, "2692", "false" }
    };
    test_cast_all<TYPE_INT, TYPE_DECIMAL64>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromIntToDecimal128p38s0Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "8485310", 38, 0, "8485310", "false" },
    {-1, -1, "1680", 38, 0, "1680", "false" },
    {-1, -1, "-1", 38, 0, "-1", "false" },
    {-1, -1, "-58", 38, 0, "-58", "false" },
    {-1, -1, "-601", 38, 0, "-601", "false" },
    {-1, -1, "3", 38, 0, "3", "false" },
    {-1, -1, "-185966", 38, 0, "-185966", "false" },
    {-1, -1, "-122013", 38, 0, "-122013", "false" },
    {-1, -1, "2125545", 38, 0, "2125545", "false" },
    {-1, -1, "-18540", 38, 0, "-18540", "false" },
    {-1, -1, "1519673621", 38, 0, "1519673621", "false" },
    {-1, -1, "-10292145", 38, 0, "-10292145", "false" },
    {-1, -1, "-2147483648", 38, 0, "-2147483648", "false" },
    {-1, -1, "637", 38, 0, "637", "false" },
    {-1, -1, "-2033303358", 38, 0, "-2033303358", "false" },
    {-1, -1, "-14978452", 38, 0, "-14978452", "false" },
    {-1, -1, "303", 38, 0, "303", "false" },
    {-1, -1, "445262", 38, 0, "445262", "false" },
    {-1, -1, "-18019199", 38, 0, "-18019199", "false" },
    {-1, -1, "29", 38, 0, "29", "false" }
    };
    test_cast_all<TYPE_INT, TYPE_DECIMAL128>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromIntToDecimal128p38s2Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "248582", 38, 2, "248582", "false" },
    {-1, -1, "-11957", 38, 2, "-11957", "false" },
    {-1, -1, "-1009", 38, 2, "-1009", "false" },
    {-1, -1, "-401", 38, 2, "-401", "false" },
    {-1, -1, "-2021694948", 38, 2, "-2021694948", "false" },
    {-1, -1, "-2147483648", 38, 2, "-2147483648", "false" },
    {-1, -1, "-18122", 38, 2, "-18122", "false" },
    {-1, -1, "28348469", 38, 2, "28348469", "false" },
    {-1, -1, "856", 38, 2, "856", "false" },
    {-1, -1, "1214723330", 38, 2, "1214723330", "false" },
    {-1, -1, "-5817574", 38, 2, "-5817574", "false" },
    {-1, -1, "11", 38, 2, "11", "false" },
    {-1, -1, "-1889083347", 38, 2, "-1889083347", "false" },
    {-1, -1, "167", 38, 2, "167", "false" },
    {-1, -1, "1949", 38, 2, "1949", "false" },
    {-1, -1, "-2", 38, 2, "-2", "false" },
    {-1, -1, "3089561", 38, 2, "3089561", "false" },
    {-1, -1, "45", 38, 2, "45", "false" },
    {-1, -1, "1757506", 38, 2, "1757506", "false" },
    {-1, -1, "62464224", 38, 2, "62464224", "false" }
    };
    test_cast_all<TYPE_INT, TYPE_DECIMAL128>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromIntToDecimal128p38s9Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "-1", 38, 9, "-1", "false" },
    {-1, -1, "-409238859", 38, 9, "-409238859", "false" },
    {-1, -1, "14", 38, 9, "14", "false" },
    {-1, -1, "-2198", 38, 9, "-2198", "false" },
    {-1, -1, "231", 38, 9, "231", "false" },
    {-1, -1, "961113", 38, 9, "961113", "false" },
    {-1, -1, "-46014687", 38, 9, "-46014687", "false" },
    {-1, -1, "-3645", 38, 9, "-3645", "false" },
    {-1, -1, "-23480", 38, 9, "-23480", "false" },
    {-1, -1, "990", 38, 9, "990", "false" },
    {-1, -1, "-1915731", 38, 9, "-1915731", "false" },
    {-1, -1, "-3", 38, 9, "-3", "false" },
    {-1, -1, "-211250", 38, 9, "-211250", "false" },
    {-1, -1, "2147483647", 38, 9, "2147483647", "false" },
    {-1, -1, "2008769", 38, 9, "2008769", "false" },
    {-1, -1, "-5", 38, 9, "-5", "false" },
    {-1, -1, "371044", 38, 9, "371044", "false" },
    {-1, -1, "1", 38, 9, "1", "false" },
    {-1, -1, "0", 38, 9, "0", "false" },
    {-1, -1, "-233", 38, 9, "-233", "false" }
    };
    test_cast_all<TYPE_INT, TYPE_DECIMAL128>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromIntToDecimal128p38s13Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "-737", 38, 13, "-737", "false" },
    {-1, -1, "546946459", 38, 13, "546946459", "false" },
    {-1, -1, "-1524338446", 38, 13, "-1524338446", "false" },
    {-1, -1, "6888296", 38, 13, "6888296", "false" },
    {-1, -1, "-5072", 38, 13, "-5072", "false" },
    {-1, -1, "355362726", 38, 13, "355362726", "false" },
    {-1, -1, "-116", 38, 13, "-116", "false" },
    {-1, -1, "430102766", 38, 13, "430102766", "false" },
    {-1, -1, "-23294", 38, 13, "-23294", "false" },
    {-1, -1, "-59576", 38, 13, "-59576", "false" },
    {-1, -1, "963", 38, 13, "963", "false" },
    {-1, -1, "664", 38, 13, "664", "false" },
    {-1, -1, "278435915", 38, 13, "278435915", "false" },
    {-1, -1, "-1896611", 38, 13, "-1896611", "false" },
    {-1, -1, "9419013", 38, 13, "9419013", "false" },
    {-1, -1, "-321", 38, 13, "-321", "false" },
    {-1, -1, "33", 38, 13, "33", "false" },
    {-1, -1, "-695", 38, 13, "-695", "false" },
    {-1, -1, "2231", 38, 13, "2231", "false" },
    {-1, -1, "1451", 38, 13, "1451", "false" }
    };
    test_cast_all<TYPE_INT, TYPE_DECIMAL128>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromIntToDecimal128p38s18Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "-6", 38, 18, "-6", "false" },
    {-1, -1, "5883", 38, 18, "5883", "false" },
    {-1, -1, "53", 38, 18, "53", "false" },
    {-1, -1, "58", 38, 18, "58", "false" },
    {-1, -1, "6", 38, 18, "6", "false" },
    {-1, -1, "-7", 38, 18, "-7", "false" },
    {-1, -1, "604365", 38, 18, "604365", "false" },
    {-1, -1, "-2671939", 38, 18, "-2671939", "false" },
    {-1, -1, "4779", 38, 18, "4779", "false" },
    {-1, -1, "22", 38, 18, "22", "false" },
    {-1, -1, "64337069", 38, 18, "64337069", "false" },
    {-1, -1, "-3883", 38, 18, "-3883", "false" },
    {-1, -1, "-18946666", 38, 18, "-18946666", "false" },
    {-1, -1, "-1813121", 38, 18, "-1813121", "false" },
    {-1, -1, "-15874716", 38, 18, "-15874716", "false" },
    {-1, -1, "1", 38, 18, "1", "false" },
    {-1, -1, "117", 38, 18, "117", "false" },
    {-1, -1, "-166527", 38, 18, "-166527", "false" },
    {-1, -1, "-223029405", 38, 18, "-223029405", "false" }
    };
    test_cast_all<TYPE_INT, TYPE_DECIMAL128>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromIntToDecimal128p35s30Abnormal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "-748697289", 35, 30, "", "true" },
    {-1, -1, "2147483647", 35, 30, "", "true" },
    {-1, -1, "-986339122", 35, 30, "", "true" }
    };
    test_cast_all_fail<TYPE_INT, TYPE_DECIMAL128>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromIntToDecimal128p35s30Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "-198107", 35, 30, "-198107", "false" },
    {-1, -1, "-10883793", 35, 30, "-10883793", "false" },
    {-1, -1, "-145888", 35, 30, "-145888", "false" },
    {-1, -1, "-46", 35, 30, "-46", "false" },
    {-1, -1, "12272", 35, 30, "12272", "false" },
    {-1, -1, "-1660173", 35, 30, "-1660173", "false" },
    {-1, -1, "-417", 35, 30, "-417", "false" },
    {-1, -1, "-143904", 35, 30, "-143904", "false" },
    {-1, -1, "0", 35, 30, "0", "false" },
    {-1, -1, "-744086", 35, 30, "-744086", "false" },
    {-1, -1, "126", 35, 30, "126", "false" },
    {-1, -1, "-43", 35, 30, "-43", "false" },
    {-1, -1, "872259", 35, 30, "872259", "false" },
    {-1, -1, "-12028863", 35, 30, "-12028863", "false" },
    {-1, -1, "136778", 35, 30, "136778", "false" },
    {-1, -1, "123", 35, 30, "123", "false" },
    {-1, -1, "71722230", 35, 30, "71722230", "false" }
    };
    test_cast_all<TYPE_INT, TYPE_DECIMAL128>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromBigIntToDecimal32p9s0Abnormal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "-733984013259202053", 9, 0, "", "true" },
    {-1, -1, "-197882106016151", 9, 0, "", "true" },
    {-1, -1, "259947125525", 9, 0, "", "true" },
    {-1, -1, "-10408008336359", 9, 0, "", "true" },
    {-1, -1, "-462224191649263049", 9, 0, "", "true" },
    {-1, -1, "-86160705728748", 9, 0, "", "true" },
    {-1, -1, "9223372036854775807", 9, 0, "", "true" },
    {-1, -1, "-1602341219224696370", 9, 0, "", "true" },
    {-1, -1, "516795814505", 9, 0, "", "true" },
    {-1, -1, "-3745071117300741288", 9, 0, "", "true" },
    {-1, -1, "-356113903416", 9, 0, "", "true" },
    {-1, -1, "217911718414", 9, 0, "", "true" }
    };
    test_cast_all_fail<TYPE_BIGINT, TYPE_DECIMAL32>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromBigIntToDecimal32p9s0Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "-6616", 9, 0, "-6616", "false" },
    {-1, -1, "1811", 9, 0, "1811", "false" },
    {-1, -1, "778203", 9, 0, "778203", "false" },
    {-1, -1, "-118224689", 9, 0, "-118224689", "false" },
    {-1, -1, "-186498", 9, 0, "-186498", "false" },
    {-1, -1, "38543792", 9, 0, "38543792", "false" },
    {-1, -1, "5237", 9, 0, "5237", "false" },
    {-1, -1, "2299133", 9, 0, "2299133", "false" }
    };
    test_cast_all<TYPE_BIGINT, TYPE_DECIMAL32>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromBigIntToDecimal32p9s2Abnormal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "-353933135708", 9, 2, "", "true" },
    {-1, -1, "364711968", 9, 2, "", "true" },
    {-1, -1, "195216752077868", 9, 2, "", "true" },
    {-1, -1, "2675318417", 9, 2, "", "true" },
    {-1, -1, "-9223372036854775808", 9, 2, "", "true" },
    {-1, -1, "-17008745101", 9, 2, "", "true" },
    {-1, -1, "-3971175760021593", 9, 2, "", "true" },
    {-1, -1, "-29988593281", 9, 2, "", "true" },
    {-1, -1, "654138363354", 9, 2, "", "true" },
    {-1, -1, "-1535953759678", 9, 2, "", "true" },
    {-1, -1, "-31598271610", 9, 2, "", "true" },
    {-1, -1, "468043761917230821", 9, 2, "", "true" },
    {-1, -1, "1081698385995689270", 9, 2, "", "true" },
    {-1, -1, "249015999", 9, 2, "", "true" }
    };
    test_cast_all_fail<TYPE_BIGINT, TYPE_DECIMAL32>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromBigIntToDecimal32p9s2Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "-4881", 9, 2, "-4881", "false" },
    {-1, -1, "-4", 9, 2, "-4", "false" },
    {-1, -1, "-6778743", 9, 2, "-6778743", "false" },
    {-1, -1, "1", 9, 2, "1", "false" },
    {-1, -1, "99", 9, 2, "99", "false" },
    {-1, -1, "-1", 9, 2, "-1", "false" }
    };
    test_cast_all<TYPE_BIGINT, TYPE_DECIMAL32>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromBigIntToDecimal32p9s9Abnormal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "15760413159295", 9, 9, "", "true" },
    {-1, -1, "-17478323023603", 9, 9, "", "true" },
    {-1, -1, "18482486126", 9, 9, "", "true" },
    {-1, -1, "-126", 9, 9, "", "true" },
    {-1, -1, "1310", 9, 9, "", "true" },
    {-1, -1, "8285979589843605196", 9, 9, "", "true" },
    {-1, -1, "1128848609696", 9, 9, "", "true" },
    {-1, -1, "1593849", 9, 9, "", "true" },
    {-1, -1, "-171801477255444", 9, 9, "", "true" },
    {-1, -1, "-6049868", 9, 9, "", "true" },
    {-1, -1, "2246492616656033318", 9, 9, "", "true" },
    {-1, -1, "-782341", 9, 9, "", "true" },
    {-1, -1, "-59421170", 9, 9, "", "true" },
    {-1, -1, "9765944689412887", 9, 9, "", "true" },
    {-1, -1, "10655", 9, 9, "", "true" },
    {-1, -1, "-182459175536", 9, 9, "", "true" },
    {-1, -1, "149724559827817141", 9, 9, "", "true" },
    {-1, -1, "131621551449", 9, 9, "", "true" },
    {-1, -1, "9223372036854775807", 9, 9, "", "true" }
    };
    test_cast_all_fail<TYPE_BIGINT, TYPE_DECIMAL32>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromBigIntToDecimal32p9s9Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "-1", 9, 9, "-1", "false" }
    };
    test_cast_all<TYPE_BIGINT, TYPE_DECIMAL32>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromBigIntToDecimal32p7s4Abnormal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "42870048660502939", 7, 4, "", "true" },
    {-1, -1, "-274099738249", 7, 4, "", "true" },
    {-1, -1, "-519157686709083", 7, 4, "", "true" },
    {-1, -1, "1217405058902", 7, 4, "", "true" },
    {-1, -1, "-220745945334030", 7, 4, "", "true" },
    {-1, -1, "-3324653", 7, 4, "", "true" },
    {-1, -1, "-21225897637", 7, 4, "", "true" },
    {-1, -1, "480118942181896228", 7, 4, "", "true" },
    {-1, -1, "-2323630856156642542", 7, 4, "", "true" },
    {-1, -1, "-1716112209", 7, 4, "", "true" },
    {-1, -1, "1523907665111", 7, 4, "", "true" }
    };
    test_cast_all_fail<TYPE_BIGINT, TYPE_DECIMAL32>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromBigIntToDecimal32p7s4Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "-12", 7, 4, "-12", "false" },
    {-1, -1, "49756", 7, 4, "49756", "false" },
    {-1, -1, "-5", 7, 4, "-5", "false" },
    {-1, -1, "3", 7, 4, "3", "false" },
    {-1, -1, "0", 7, 4, "0", "false" },
    {-1, -1, "-20656", 7, 4, "-20656", "false" },
    {-1, -1, "-3", 7, 4, "-3", "false" },
    {-1, -1, "-68840", 7, 4, "-68840", "false" },
    {-1, -1, "-15980", 7, 4, "-15980", "false" }
    };
    test_cast_all<TYPE_BIGINT, TYPE_DECIMAL32>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromBigIntToDecimal64p18s0Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "-1974", 18, 0, "-1974", "false" },
    {-1, -1, "-14925026147879784", 18, 0, "-14925026147879784", "false" },
    {-1, -1, "172324410650943412", 18, 0, "172324410650943412", "false" },
    {-1, -1, "124", 18, 0, "124", "false" },
    {-1, -1, "7849305327814618757", 18, 0, "7849305327814618757", "false" },
    {-1, -1, "-2830005376", 18, 0, "-2830005376", "false" },
    {-1, -1, "11329724685312", 18, 0, "11329724685312", "false" },
    {-1, -1, "-8255516120407247398", 18, 0, "-8255516120407247398", "false" },
    {-1, -1, "3", 18, 0, "3", "false" },
    {-1, -1, "312021644", 18, 0, "312021644", "false" },
    {-1, -1, "1128035588", 18, 0, "1128035588", "false" },
    {-1, -1, "7216539226874589115", 18, 0, "7216539226874589115", "false" },
    {-1, -1, "-15023446147478786", 18, 0, "-15023446147478786", "false" },
    {-1, -1, "-277179143896665", 18, 0, "-277179143896665", "false" },
    {-1, -1, "2558208717467867544", 18, 0, "2558208717467867544", "false" },
    {-1, -1, "945372241366397332", 18, 0, "945372241366397332", "false" },
    {-1, -1, "42188722", 18, 0, "42188722", "false" },
    {-1, -1, "-14290056576", 18, 0, "-14290056576", "false" },
    {-1, -1, "-2589888873976125833", 18, 0, "-2589888873976125833", "false" },
    {-1, -1, "39878571", 18, 0, "39878571", "false" }
    };
    test_cast_all<TYPE_BIGINT, TYPE_DECIMAL64>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromBigIntToDecimal64p18s2Abnormal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "-412373022386724108", 18, 2, "", "true" }
    };
    test_cast_all_fail<TYPE_BIGINT, TYPE_DECIMAL64>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromBigIntToDecimal64p18s2Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "179111447946435", 18, 2, "179111447946435", "false" },
    {-1, -1, "-14594", 18, 2, "-14594", "false" },
    {-1, -1, "8706", 18, 2, "8706", "false" },
    {-1, -1, "2914", 18, 2, "2914", "false" },
    {-1, -1, "-599476969", 18, 2, "-599476969", "false" },
    {-1, -1, "5145045068674", 18, 2, "5145045068674", "false" },
    {-1, -1, "-41519", 18, 2, "-41519", "false" },
    {-1, -1, "-3644732666", 18, 2, "-3644732666", "false" },
    {-1, -1, "-390810026919", 18, 2, "-390810026919", "false" },
    {-1, -1, "-499", 18, 2, "-499", "false" },
    {-1, -1, "6378332621", 18, 2, "6378332621", "false" },
    {-1, -1, "5205090", 18, 2, "5205090", "false" },
    {-1, -1, "6855848114", 18, 2, "6855848114", "false" },
    {-1, -1, "6971", 18, 2, "6971", "false" },
    {-1, -1, "-140847673350", 18, 2, "-140847673350", "false" },
    {-1, -1, "-16733509", 18, 2, "-16733509", "false" },
    {-1, -1, "1812", 18, 2, "1812", "false" },
    {-1, -1, "8644264694295797", 18, 2, "8644264694295797", "false" },
    {-1, -1, "23", 18, 2, "23", "false" }
    };
    test_cast_all<TYPE_BIGINT, TYPE_DECIMAL64>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromBigIntToDecimal64p18s9Abnormal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "21382250286", 18, 9, "", "true" },
    {-1, -1, "3197082398318004286", 18, 9, "", "true" },
    {-1, -1, "199265761549663941", 18, 9, "", "true" },
    {-1, -1, "-22891290831", 18, 9, "", "true" },
    {-1, -1, "5678888406492249504", 18, 9, "", "true" },
    {-1, -1, "-71184471751781", 18, 9, "", "true" },
    {-1, -1, "1104332740609237", 18, 9, "", "true" },
    {-1, -1, "-18015570166931609", 18, 9, "", "true" },
    {-1, -1, "8770113441290752", 18, 9, "", "true" },
    {-1, -1, "-4782003868400919", 18, 9, "", "true" }
    };
    test_cast_all_fail<TYPE_BIGINT, TYPE_DECIMAL64>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromBigIntToDecimal64p18s9Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "-159812317", 18, 9, "-159812317", "false" },
    {-1, -1, "841", 18, 9, "841", "false" },
    {-1, -1, "229369593", 18, 9, "229369593", "false" },
    {-1, -1, "268089", 18, 9, "268089", "false" },
    {-1, -1, "5", 18, 9, "5", "false" },
    {-1, -1, "17", 18, 9, "17", "false" },
    {-1, -1, "3082552", 18, 9, "3082552", "false" },
    {-1, -1, "25875", 18, 9, "25875", "false" },
    {-1, -1, "-486", 18, 9, "-486", "false" },
    {-1, -1, "20625", 18, 9, "20625", "false" }
    };
    test_cast_all<TYPE_BIGINT, TYPE_DECIMAL64>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromBigIntToDecimal64p18s18Abnormal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "305337316873", 18, 18, "", "true" },
    {-1, -1, "-4388869870456221", 18, 18, "", "true" },
    {-1, -1, "41322", 18, 18, "", "true" },
    {-1, -1, "-220090", 18, 18, "", "true" },
    {-1, -1, "-72002", 18, 18, "", "true" },
    {-1, -1, "61395724220436", 18, 18, "", "true" },
    {-1, -1, "523999425680907", 18, 18, "", "true" },
    {-1, -1, "-2100191147411059", 18, 18, "", "true" },
    {-1, -1, "1131753037331940529", 18, 18, "", "true" },
    {-1, -1, "-30027", 18, 18, "", "true" },
    {-1, -1, "-5639734362148487318", 18, 18, "", "true" },
    {-1, -1, "-2801059", 18, 18, "", "true" },
    {-1, -1, "-13209231438", 18, 18, "", "true" },
    {-1, -1, "179", 18, 18, "", "true" },
    {-1, -1, "34176403852285738", 18, 18, "", "true" },
    {-1, -1, "-648737385742840962", 18, 18, "", "true" },
    {-1, -1, "-25517741492612", 18, 18, "", "true" },
    {-1, -1, "177668399212503152", 18, 18, "", "true" },
    {-1, -1, "-192772", 18, 18, "", "true" },
    {-1, -1, "14467455091", 18, 18, "", "true" }
    };
    test_cast_all_fail<TYPE_BIGINT, TYPE_DECIMAL64>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromBigIntToDecimal64p15s13Abnormal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "-3681811", 15, 13, "", "true" },
    {-1, -1, "-2264056", 15, 13, "", "true" },
    {-1, -1, "-1781764477213701", 15, 13, "", "true" },
    {-1, -1, "1345487299267996966", 15, 13, "", "true" },
    {-1, -1, "-4604302928906084317", 15, 13, "", "true" },
    {-1, -1, "1003444568", 15, 13, "", "true" },
    {-1, -1, "-126737963452221", 15, 13, "", "true" },
    {-1, -1, "-712326662382", 15, 13, "", "true" },
    {-1, -1, "-12377153", 15, 13, "", "true" },
    {-1, -1, "13027716284", 15, 13, "", "true" },
    {-1, -1, "8095493810579", 15, 13, "", "true" },
    {-1, -1, "-18216055137849302", 15, 13, "", "true" },
    {-1, -1, "-17225619128", 15, 13, "", "true" },
    {-1, -1, "-3470333680617014303", 15, 13, "", "true" },
    {-1, -1, "-50146801066", 15, 13, "", "true" },
    {-1, -1, "-130939740614", 15, 13, "", "true" }
    };
    test_cast_all_fail<TYPE_BIGINT, TYPE_DECIMAL64>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromBigIntToDecimal64p15s13Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "-1", 15, 13, "-1", "false" },
    {-1, -1, "8061", 15, 13, "8061", "false" },
    {-1, -1, "0", 15, 13, "0", "false" },
    {-1, -1, "1", 15, 13, "1", "false" }
    };
    test_cast_all<TYPE_BIGINT, TYPE_DECIMAL64>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromBigIntToDecimal128p38s0Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "12184370", 38, 0, "12184370", "false" },
    {-1, -1, "878103961826", 38, 0, "878103961826", "false" },
    {-1, -1, "-340", 38, 0, "-340", "false" },
    {-1, -1, "-14", 38, 0, "-14", "false" },
    {-1, -1, "-1139577992031", 38, 0, "-1139577992031", "false" },
    {-1, -1, "69084311", 38, 0, "69084311", "false" },
    {-1, -1, "11019", 38, 0, "11019", "false" },
    {-1, -1, "435022802286", 38, 0, "435022802286", "false" },
    {-1, -1, "47144337135213892", 38, 0, "47144337135213892", "false" },
    {-1, -1, "-36924819961968062", 38, 0, "-36924819961968062", "false" },
    {-1, -1, "-16246094", 38, 0, "-16246094", "false" },
    {-1, -1, "-225030", 38, 0, "-225030", "false" },
    {-1, -1, "3275516654622819034", 38, 0, "3275516654622819034", "false" },
    {-1, -1, "612322341302", 38, 0, "612322341302", "false" },
    {-1, -1, "-14374", 38, 0, "-14374", "false" },
    {-1, -1, "-8067746", 38, 0, "-8067746", "false" },
    {-1, -1, "831", 38, 0, "831", "false" },
    {-1, -1, "165818982476990", 38, 0, "165818982476990", "false" },
    {-1, -1, "356131", 38, 0, "356131", "false" },
    {-1, -1, "171244", 38, 0, "171244", "false" }
    };
    test_cast_all<TYPE_BIGINT, TYPE_DECIMAL128>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromBigIntToDecimal128p38s2Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "914166", 38, 2, "914166", "false" },
    {-1, -1, "2107037338649474", 38, 2, "2107037338649474", "false" },
    {-1, -1, "-232", 38, 2, "-232", "false" },
    {-1, -1, "-25002268", 38, 2, "-25002268", "false" },
    {-1, -1, "-15273", 38, 2, "-15273", "false" },
    {-1, -1, "41190", 38, 2, "41190", "false" },
    {-1, -1, "6068624473", 38, 2, "6068624473", "false" },
    {-1, -1, "0", 38, 2, "0", "false" },
    {-1, -1, "-4063846740494", 38, 2, "-4063846740494", "false" },
    {-1, -1, "-35334037394262", 38, 2, "-35334037394262", "false" },
    {-1, -1, "-395", 38, 2, "-395", "false" },
    {-1, -1, "1333150781711", 38, 2, "1333150781711", "false" },
    {-1, -1, "-508", 38, 2, "-508", "false" },
    {-1, -1, "-101514001", 38, 2, "-101514001", "false" },
    {-1, -1, "-1964380486705261", 38, 2, "-1964380486705261", "false" },
    {-1, -1, "-1033999268", 38, 2, "-1033999268", "false" },
    {-1, -1, "-9390426277876294", 38, 2, "-9390426277876294", "false" },
    {-1, -1, "55948830677156", 38, 2, "55948830677156", "false" },
    {-1, -1, "94046904658", 38, 2, "94046904658", "false" },
    {-1, -1, "-7058378862893", 38, 2, "-7058378862893", "false" }
    };
    test_cast_all<TYPE_BIGINT, TYPE_DECIMAL128>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromBigIntToDecimal128p38s9Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "-179", 38, 9, "-179", "false" },
    {-1, -1, "1", 38, 9, "1", "false" },
    {-1, -1, "13329206540195", 38, 9, "13329206540195", "false" },
    {-1, -1, "-35058645544198221", 38, 9, "-35058645544198221", "false" },
    {-1, -1, "-2724491852580098", 38, 9, "-2724491852580098", "false" },
    {-1, -1, "679697", 38, 9, "679697", "false" },
    {-1, -1, "-3135", 38, 9, "-3135", "false" },
    {-1, -1, "3974069415730328", 38, 9, "3974069415730328", "false" },
    {-1, -1, "934411227004398", 38, 9, "934411227004398", "false" },
    {-1, -1, "-131736365", 38, 9, "-131736365", "false" },
    {-1, -1, "-272072615418234", 38, 9, "-272072615418234", "false" },
    {-1, -1, "-49022838572", 38, 9, "-49022838572", "false" },
    {-1, -1, "101293027", 38, 9, "101293027", "false" },
    {-1, -1, "-16413535978644767", 38, 9, "-16413535978644767", "false" },
    {-1, -1, "-3437675199", 38, 9, "-3437675199", "false" },
    {-1, -1, "402788844280", 38, 9, "402788844280", "false" },
    {-1, -1, "234", 38, 9, "234", "false" },
    {-1, -1, "-7590889761629", 38, 9, "-7590889761629", "false" },
    {-1, -1, "-17313715877", 38, 9, "-17313715877", "false" },
    {-1, -1, "-18907769706170", 38, 9, "-18907769706170", "false" }
    };
    test_cast_all<TYPE_BIGINT, TYPE_DECIMAL128>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromBigIntToDecimal128p38s13Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "7681", 38, 13, "7681", "false" },
    {-1, -1, "-15837778823339578", 38, 13, "-15837778823339578", "false" },
    {-1, -1, "24510", 38, 13, "24510", "false" },
    {-1, -1, "-3473312", 38, 13, "-3473312", "false" },
    {-1, -1, "1977588737026", 38, 13, "1977588737026", "false" },
    {-1, -1, "-4244295622407381448", 38, 13, "-4244295622407381448", "false" },
    {-1, -1, "-4381085421596480903", 38, 13, "-4381085421596480903", "false" },
    {-1, -1, "-1318528072030205666", 38, 13, "-1318528072030205666", "false" },
    {-1, -1, "89523165873", 38, 13, "89523165873", "false" },
    {-1, -1, "-161725898697", 38, 13, "-161725898697", "false" },
    {-1, -1, "83719240465175406", 38, 13, "83719240465175406", "false" },
    {-1, -1, "-567496356049075521", 38, 13, "-567496356049075521", "false" },
    {-1, -1, "-8994", 38, 13, "-8994", "false" },
    {-1, -1, "-116412430", 38, 13, "-116412430", "false" },
    {-1, -1, "-112103684549713392", 38, 13, "-112103684549713392", "false" },
    {-1, -1, "-16231031759207355", 38, 13, "-16231031759207355", "false" },
    {-1, -1, "-41439754398", 38, 13, "-41439754398", "false" },
    {-1, -1, "17059334", 38, 13, "17059334", "false" },
    {-1, -1, "-638", 38, 13, "-638", "false" },
    {-1, -1, "131244561008814761", 38, 13, "131244561008814761", "false" }
    };
    test_cast_all<TYPE_BIGINT, TYPE_DECIMAL128>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromBigIntToDecimal128p38s18Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "-109702231730974488", 38, 18, "-109702231730974488", "false" },
    {-1, -1, "683005368", 38, 18, "683005368", "false" },
    {-1, -1, "-83560673685185", 38, 18, "-83560673685185", "false" },
    {-1, -1, "-50944774462152219", 38, 18, "-50944774462152219", "false" },
    {-1, -1, "-230", 38, 18, "-230", "false" },
    {-1, -1, "444290419114761", 38, 18, "444290419114761", "false" },
    {-1, -1, "15833257119", 38, 18, "15833257119", "false" },
    {-1, -1, "12189670539804", 38, 18, "12189670539804", "false" },
    {-1, -1, "-447815519625171821", 38, 18, "-447815519625171821", "false" },
    {-1, -1, "33402132", 38, 18, "33402132", "false" },
    {-1, -1, "-191633224628988667", 38, 18, "-191633224628988667", "false" },
    {-1, -1, "388121562", 38, 18, "388121562", "false" },
    {-1, -1, "29093869524", 38, 18, "29093869524", "false" },
    {-1, -1, "-9223372036854775808", 38, 18, "-9223372036854775808", "false" },
    {-1, -1, "-31389", 38, 18, "-31389", "false" },
    {-1, -1, "-42529094243860", 38, 18, "-42529094243860", "false" },
    {-1, -1, "9878436", 38, 18, "9878436", "false" },
    {-1, -1, "32772618", 38, 18, "32772618", "false" },
    {-1, -1, "1", 38, 18, "1", "false" },
    {-1, -1, "-7902596896671", 38, 18, "-7902596896671", "false" }
    };
    test_cast_all<TYPE_BIGINT, TYPE_DECIMAL128>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromBigIntToDecimal128p35s30Abnormal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "-161790336676902630", 35, 30, "", "true" },
    {-1, -1, "-11150795461", 35, 30, "", "true" },
    {-1, -1, "-1451840505892", 35, 30, "", "true" },
    {-1, -1, "4443038211311", 35, 30, "", "true" },
    {-1, -1, "-4136849619", 35, 30, "", "true" },
    {-1, -1, "4304413230558850309", 35, 30, "", "true" },
    {-1, -1, "22774176560", 35, 30, "", "true" },
    {-1, -1, "85884874313", 35, 30, "", "true" },
    {-1, -1, "-10279843118", 35, 30, "", "true" },
    {-1, -1, "87391488815", 35, 30, "", "true" },
    {-1, -1, "-7527191764827792359", 35, 30, "", "true" }
    };
    test_cast_all_fail<TYPE_BIGINT, TYPE_DECIMAL128>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromBigIntToDecimal128p35s30Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "18741679", 35, 30, "18741679", "false" },
    {-1, -1, "-120", 35, 30, "-120", "false" },
    {-1, -1, "-147012", 35, 30, "-147012", "false" },
    {-1, -1, "899", 35, 30, "899", "false" },
    {-1, -1, "-4", 35, 30, "-4", "false" },
    {-1, -1, "-736592", 35, 30, "-736592", "false" },
    {-1, -1, "3099", 35, 30, "3099", "false" },
    {-1, -1, "-6307792", 35, 30, "-6307792", "false" },
    {-1, -1, "-1", 35, 30, "-1", "false" }
    };
    test_cast_all<TYPE_BIGINT, TYPE_DECIMAL128>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromLargeIntToDecimal32p9s0Abnormal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "1676482344522828658", 9, 0, "", "true" },
    {-1, -1, "-18093261755332702448689712513680", 9, 0, "", "true" },
    {-1, -1, "-14455250057692942697852419673", 9, 0, "", "true" },
    {-1, -1, "-27931438097545444035011350586364698358", 9, 0, "", "true" },
    {-1, -1, "-7263018049238490356", 9, 0, "", "true" },
    {-1, -1, "-2582066969131091031", 9, 0, "", "true" },
    {-1, -1, "-1042378719022", 9, 0, "", "true" },
    {-1, -1, "225762724389744161747758898772262862", 9, 0, "", "true" },
    {-1, -1, "-22498806704125", 9, 0, "", "true" },
    {-1, -1, "20243735492346663973672075962", 9, 0, "", "true" },
    {-1, -1, "928594783026213698758235670210449385", 9, 0, "", "true" },
    {-1, -1, "-97691195596879", 9, 0, "", "true" },
    {-1, -1, "-136056938388", 9, 0, "", "true" },
    {-1, -1, "193551204523319338217088121386566", 9, 0, "", "true" }
    };
    test_cast_all_fail<TYPE_LARGEINT, TYPE_DECIMAL32>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromLargeIntToDecimal32p9s0Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "-9", 9, 0, "-9", "false" },
    {-1, -1, "-11", 9, 0, "-11", "false" },
    {-1, -1, "-77722434", 9, 0, "-77722434", "false" },
    {-1, -1, "591132320", 9, 0, "591132320", "false" },
    {-1, -1, "2035825664", 9, 0, "2035825664", "false" },
    {-1, -1, "-14569", 9, 0, "-14569", "false" }
    };
    test_cast_all<TYPE_LARGEINT, TYPE_DECIMAL32>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromLargeIntToDecimal32p9s2Abnormal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "-800456383379702", 9, 2, "", "true" },
    {-1, -1, "-3883566155320", 9, 2, "", "true" },
    {-1, -1, "-40944307189449407411253468461140142450", 9, 2, "", "true" },
    {-1, -1, "254325609617253350553", 9, 2, "", "true" },
    {-1, -1, "-134070300657325", 9, 2, "", "true" },
    {-1, -1, "-491304509891018168405579", 9, 2, "", "true" },
    {-1, -1, "576830332982829204485216", 9, 2, "", "true" },
    {-1, -1, "-15202765988617075027301328583763761047", 9, 2, "", "true" },
    {-1, -1, "38678368426191991637189562944", 9, 2, "", "true" },
    {-1, -1, "-8200825181", 9, 2, "", "true" },
    {-1, -1, "1528656750598428692967247419718293760", 9, 2, "", "true" },
    {-1, -1, "238229426224", 9, 2, "", "true" },
    {-1, -1, "-112804016757749035380", 9, 2, "", "true" },
    {-1, -1, "525520324001073", 9, 2, "", "true" },
    {-1, -1, "-53978834236520440099208463949928153695", 9, 2, "", "true" },
    {-1, -1, "-80791383", 9, 2, "", "true" }
    };
    test_cast_all_fail<TYPE_LARGEINT, TYPE_DECIMAL32>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromLargeIntToDecimal32p9s2Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "-62", 9, 2, "-62", "false" },
    {-1, -1, "-24", 9, 2, "-24", "false" },
    {-1, -1, "-130635", 9, 2, "-130635", "false" },
    {-1, -1, "-1028", 9, 2, "-1028", "false" }
    };
    test_cast_all<TYPE_LARGEINT, TYPE_DECIMAL32>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromLargeIntToDecimal32p9s9Abnormal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "1998973441645971128278670936925748075", 9, 9, "", "true" },
    {-1, -1, "-140722583421365178", 9, 9, "", "true" },
    {-1, -1, "53679927133740915488306", 9, 9, "", "true" },
    {-1, -1, "17662420342", 9, 9, "", "true" },
    {-1, -1, "-90545428523170628736889430552917", 9, 9, "", "true" },
    {-1, -1, "-52095867081901874531180707", 9, 9, "", "true" },
    {-1, -1, "-2044372037631", 9, 9, "", "true" },
    {-1, -1, "-5030953018761233249761820149658", 9, 9, "", "true" },
    {-1, -1, "-91104837213246326101614476380690", 9, 9, "", "true" },
    {-1, -1, "-1865263856888230271955880", 9, 9, "", "true" },
    {-1, -1, "-28", 9, 9, "", "true" },
    {-1, -1, "-4268084040010132272830069924", 9, 9, "", "true" },
    {-1, -1, "7310777828650", 9, 9, "", "true" },
    {-1, -1, "-37333", 9, 9, "", "true" },
    {-1, -1, "-48900", 9, 9, "", "true" },
    {-1, -1, "335", 9, 9, "", "true" },
    {-1, -1, "-81714963547030", 9, 9, "", "true" },
    {-1, -1, "-70825383443955270641644274823402144736", 9, 9, "", "true" },
    {-1, -1, "130107545632715191485226178957007", 9, 9, "", "true" },
    {-1, -1, "661376787138245226417157948930091190", 9, 9, "", "true" }
    };
    test_cast_all_fail<TYPE_LARGEINT, TYPE_DECIMAL32>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromLargeIntToDecimal32p7s4Abnormal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "1186559", 7, 4, "", "true" },
    {-1, -1, "84501288035073308588421057783645", 7, 4, "", "true" },
    {-1, -1, "2020674441641292248146516", 7, 4, "", "true" },
    {-1, -1, "-9000201543451988539", 7, 4, "", "true" },
    {-1, -1, "-62890794043001", 7, 4, "", "true" },
    {-1, -1, "-4105083193", 7, 4, "", "true" },
    {-1, -1, "-249046057", 7, 4, "", "true" },
    {-1, -1, "-125889701745934403305391391488063024", 7, 4, "", "true" },
    {-1, -1, "70034737129122", 7, 4, "", "true" },
    {-1, -1, "15484138052390", 7, 4, "", "true" },
    {-1, -1, "-61456688140133595431143941392923737", 7, 4, "", "true" },
    {-1, -1, "13491343199034108019814078596871861", 7, 4, "", "true" },
    {-1, -1, "-4287247898239355037689776643", 7, 4, "", "true" },
    {-1, -1, "1971618252483211646756035894", 7, 4, "", "true" },
    {-1, -1, "-103439784672753", 7, 4, "", "true" },
    {-1, -1, "23893773106116498617417336", 7, 4, "", "true" },
    {-1, -1, "-568336461954919202086966943318289", 7, 4, "", "true" },
    {-1, -1, "-6384500205033938613505", 7, 4, "", "true" },
    {-1, -1, "-18132171973128469497057645983558592608", 7, 4, "", "true" }
    };
    test_cast_all_fail<TYPE_LARGEINT, TYPE_DECIMAL32>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromLargeIntToDecimal32p7s4Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "925", 7, 4, "925", "false" }
    };
    test_cast_all<TYPE_LARGEINT, TYPE_DECIMAL32>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromLargeIntToDecimal64p18s0Abnormal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "-2000351536136662926082078817304105", 18, 0, "", "true" },
    {-1, -1, "-7664385617096669113485065081052", 18, 0, "", "true" },
    {-1, -1, "-422237204265386893724263358642014", 18, 0, "", "true" },
    {-1, -1, "8611102500509080365878660", 18, 0, "", "true" },
    {-1, -1, "-22238229136399558052551991088173766", 18, 0, "", "true" },
    {-1, -1, "-6677501886515724416690120176778", 18, 0, "", "true" },
    {-1, -1, "15795073006098626894381829", 18, 0, "", "true" },
    {-1, -1, "161012172277196340820371", 18, 0, "", "true" }
    };
    test_cast_all_fail<TYPE_LARGEINT, TYPE_DECIMAL64>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromLargeIntToDecimal64p18s0Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "0", 18, 0, "0", "false" },
    {-1, -1, "68569751589646", 18, 0, "68569751589646", "false" },
    {-1, -1, "-72230133836856089", 18, 0, "-72230133836856089", "false" },
    {-1, -1, "-54393308199132731", 18, 0, "-54393308199132731", "false" },
    {-1, -1, "945146", 18, 0, "945146", "false" },
    {-1, -1, "8114474083", 18, 0, "8114474083", "false" },
    {-1, -1, "304044382111029", 18, 0, "304044382111029", "false" },
    {-1, -1, "6991240578188397544", 18, 0, "6991240578188397544", "false" },
    {-1, -1, "501535", 18, 0, "501535", "false" },
    {-1, -1, "2193276", 18, 0, "2193276", "false" },
    {-1, -1, "479229", 18, 0, "479229", "false" },
    {-1, -1, "483131176200", 18, 0, "483131176200", "false" }
    };
    test_cast_all<TYPE_LARGEINT, TYPE_DECIMAL64>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromLargeIntToDecimal64p18s2Abnormal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "473126982069097391079495498614244102", 18, 2, "", "true" },
    {-1, -1, "6135703342614741443794676", 18, 2, "", "true" },
    {-1, -1, "-6523182115153476321514884129505016316", 18, 2, "", "true" },
    {-1, -1, "-366986813498400921064633304833578030", 18, 2, "", "true" },
    {-1, -1, "-33007337729466645096380", 18, 2, "", "true" },
    {-1, -1, "-222867309396595365501", 18, 2, "", "true" },
    {-1, -1, "-10411037787589832972082502769443", 18, 2, "", "true" },
    {-1, -1, "-45189328094000607690213018234078", 18, 2, "", "true" },
    {-1, -1, "-4921057260909991187250", 18, 2, "", "true" },
    {-1, -1, "-838028860297487232", 18, 2, "", "true" },
    {-1, -1, "-27575849748989061846", 18, 2, "", "true" },
    {-1, -1, "-1245549527302707719", 18, 2, "", "true" },
    {-1, -1, "503236201311679543", 18, 2, "", "true" }
    };
    test_cast_all_fail<TYPE_LARGEINT, TYPE_DECIMAL64>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromLargeIntToDecimal64p18s2Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "1541897", 18, 2, "1541897", "false" },
    {-1, -1, "954103930", 18, 2, "954103930", "false" },
    {-1, -1, "121203", 18, 2, "121203", "false" },
    {-1, -1, "-7582", 18, 2, "-7582", "false" },
    {-1, -1, "-719342", 18, 2, "-719342", "false" },
    {-1, -1, "-1172912847", 18, 2, "-1172912847", "false" },
    {-1, -1, "100653239307", 18, 2, "100653239307", "false" }
    };
    test_cast_all<TYPE_LARGEINT, TYPE_DECIMAL64>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromLargeIntToDecimal64p18s9Abnormal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "998841958412415680107326840664547618", 18, 9, "", "true" },
    {-1, -1, "836135994104222731685568", 18, 9, "", "true" },
    {-1, -1, "7061594249990290", 18, 9, "", "true" },
    {-1, -1, "1173616550967457756439437274", 18, 9, "", "true" },
    {-1, -1, "-127283203089", 18, 9, "", "true" },
    {-1, -1, "13788763169371546527132", 18, 9, "", "true" },
    {-1, -1, "-105624770733249786", 18, 9, "", "true" },
    {-1, -1, "-13203522812181857447958297943897284347", 18, 9, "", "true" },
    {-1, -1, "3789418108896731035949616811", 18, 9, "", "true" },
    {-1, -1, "-1402490618506", 18, 9, "", "true" },
    {-1, -1, "2806854793120881721638868285819", 18, 9, "", "true" }
    };
    test_cast_all_fail<TYPE_LARGEINT, TYPE_DECIMAL64>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromLargeIntToDecimal64p18s9Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "263763515", 18, 9, "263763515", "false" },
    {-1, -1, "1317", 18, 9, "1317", "false" },
    {-1, -1, "-5067982", 18, 9, "-5067982", "false" },
    {-1, -1, "-5018", 18, 9, "-5018", "false" },
    {-1, -1, "-179", 18, 9, "-179", "false" },
    {-1, -1, "3253", 18, 9, "3253", "false" },
    {-1, -1, "3160553", 18, 9, "3160553", "false" },
    {-1, -1, "123728", 18, 9, "123728", "false" },
    {-1, -1, "-480", 18, 9, "-480", "false" }
    };
    test_cast_all<TYPE_LARGEINT, TYPE_DECIMAL64>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromLargeIntToDecimal64p18s18Abnormal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "-38518761103363359583480056", 18, 18, "", "true" },
    {-1, -1, "2408304", 18, 18, "", "true" },
    {-1, -1, "-32599406424612823064450529900349038", 18, 18, "", "true" },
    {-1, -1, "-2930827326601731", 18, 18, "", "true" },
    {-1, -1, "464832979573278845096169636", 18, 18, "", "true" },
    {-1, -1, "135704872465789559", 18, 18, "", "true" },
    {-1, -1, "-15140573", 18, 18, "", "true" },
    {-1, -1, "2983591254947906096674933196853135", 18, 18, "", "true" },
    {-1, -1, "-106815515", 18, 18, "", "true" },
    {-1, -1, "847098", 18, 18, "", "true" },
    {-1, -1, "37994306283106692899", 18, 18, "", "true" },
    {-1, -1, "-214173371974478527619247", 18, 18, "", "true" },
    {-1, -1, "12680015907250", 18, 18, "", "true" },
    {-1, -1, "5473330337793080936496409933", 18, 18, "", "true" },
    {-1, -1, "-1211395564540258", 18, 18, "", "true" },
    {-1, -1, "-10258068579158527088230", 18, 18, "", "true" },
    {-1, -1, "485801798781388881992168458", 18, 18, "", "true" },
    {-1, -1, "14078117124386391553524335634120", 18, 18, "", "true" },
    {-1, -1, "1366", 18, 18, "", "true" }
    };
    test_cast_all_fail<TYPE_LARGEINT, TYPE_DECIMAL64>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromLargeIntToDecimal64p18s18Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "1", 18, 18, "1", "false" }
    };
    test_cast_all<TYPE_LARGEINT, TYPE_DECIMAL64>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromLargeIntToDecimal64p15s13Abnormal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "-1187309854732957625686829410420280", 15, 13, "", "true" },
    {-1, -1, "-46781607720146983764539139", 15, 13, "", "true" },
    {-1, -1, "52803731496543089233860806703993248791", 15, 13, "", "true" },
    {-1, -1, "6280496730769958823351927947788", 15, 13, "", "true" },
    {-1, -1, "-19950374555593412941160838939648", 15, 13, "", "true" },
    {-1, -1, "1388131348575083375923125141", 15, 13, "", "true" },
    {-1, -1, "76497013808042850583037574759", 15, 13, "", "true" },
    {-1, -1, "-17717733571985059465007776276", 15, 13, "", "true" },
    {-1, -1, "-5970031065124", 15, 13, "", "true" },
    {-1, -1, "-106751078", 15, 13, "", "true" },
    {-1, -1, "-1198086383854826919125148869272880", 15, 13, "", "true" },
    {-1, -1, "147079341622694271980419088", 15, 13, "", "true" },
    {-1, -1, "149933413606218524394408134", 15, 13, "", "true" },
    {-1, -1, "-21593108427623756893751670471977796", 15, 13, "", "true" },
    {-1, -1, "-27746375569160308900", 15, 13, "", "true" },
    {-1, -1, "3893212827135408", 15, 13, "", "true" },
    {-1, -1, "76852965852263125772630453", 15, 13, "", "true" },
    {-1, -1, "118082305862", 15, 13, "", "true" }
    };
    test_cast_all_fail<TYPE_LARGEINT, TYPE_DECIMAL64>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromLargeIntToDecimal64p15s13Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "-868634", 15, 13, "-868634", "false" },
    {-1, -1, "166722", 15, 13, "166722", "false" }
    };
    test_cast_all<TYPE_LARGEINT, TYPE_DECIMAL64>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromLargeIntToDecimal128p38s0Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "69858553529707272281913149793561", 38, 0, "69858553529707272281913149793561", "false" },
    {-1, -1, "-1179805859395930483380212", 38, 0, "-1179805859395930483380212", "false" },
    {-1, -1, "2284183992175260930642457", 38, 0, "2284183992175260930642457", "false" },
    {-1, -1, "6311866999769588329531794229", 38, 0, "6311866999769588329531794229", "false" },
    {-1, -1, "-3592783974973565163443817698106578910", 38, 0, "-3592783974973565163443817698106578910", "false" },
    {-1, -1, "-10056408051233328263734664723118", 38, 0, "-10056408051233328263734664723118", "false" },
    {-1, -1, "-208408494567", 38, 0, "-208408494567", "false" },
    {-1, -1, "-3795663700", 38, 0, "-3795663700", "false" },
    {-1, -1, "-12549846670108987879596989353907039", 38, 0, "-12549846670108987879596989353907039", "false" },
    {-1, -1, "-275305952891146326675883424", 38, 0, "-275305952891146326675883424", "false" },
    {-1, -1, "1640005337522", 38, 0, "1640005337522", "false" },
    {-1, -1, "-22739734090824", 38, 0, "-22739734090824", "false" },
    {-1, -1, "281693382546060", 38, 0, "281693382546060", "false" },
    {-1, -1, "3007917565928411", 38, 0, "3007917565928411", "false" },
    {-1, -1, "-217467857650244216285373361", 38, 0, "-217467857650244216285373361", "false" },
    {-1, -1, "131003", 38, 0, "131003", "false" },
    {-1, -1, "-280481403382125147872490253", 38, 0, "-280481403382125147872490253", "false" },
    {-1, -1, "-1433742084941", 38, 0, "-1433742084941", "false" },
    {-1, -1, "-2852940771", 38, 0, "-2852940771", "false" },
    {-1, -1, "-411830757620010534536912545767", 38, 0, "-411830757620010534536912545767", "false" }
    };
    test_cast_all<TYPE_LARGEINT, TYPE_DECIMAL128>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromLargeIntToDecimal128p38s2Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "-44949738320684393446381079403132", 38, 2, "-44949738320684393446381079403132", "false" },
    {-1, -1, "-635695", 38, 2, "-635695", "false" },
    {-1, -1, "7902586784815788183711", 38, 2, "7902586784815788183711", "false" },
    {-1, -1, "-799227007140241171515163713", 38, 2, "-799227007140241171515163713", "false" },
    {-1, -1, "-67906", 38, 2, "-67906", "false" },
    {-1, -1, "-735811", 38, 2, "-735811", "false" },
    {-1, -1, "-3484755542661771373222918", 38, 2, "-3484755542661771373222918", "false" },
    {-1, -1, "-44972485", 38, 2, "-44972485", "false" },
    {-1, -1, "-57919815200062923", 38, 2, "-57919815200062923", "false" },
    {-1, -1, "-529832000872681657727", 38, 2, "-529832000872681657727", "false" },
    {-1, -1, "0", 38, 2, "0", "false" },
    {-1, -1, "100651904606093062485", 38, 2, "100651904606093062485", "false" },
    {-1, -1, "21820657347", 38, 2, "21820657347", "false" },
    {-1, -1, "-871590582251775287", 38, 2, "-871590582251775287", "false" },
    {-1, -1, "-446734833816050164", 38, 2, "-446734833816050164", "false" },
    {-1, -1, "16620372132040573360696129227", 38, 2, "16620372132040573360696129227", "false" },
    {-1, -1, "-105351571835461", 38, 2, "-105351571835461", "false" },
    {-1, -1, "476", 38, 2, "476", "false" },
    {-1, -1, "-9194311993090500720", 38, 2, "-9194311993090500720", "false" },
    {-1, -1, "-6416289964", 38, 2, "-6416289964", "false" }
    };
    test_cast_all<TYPE_LARGEINT, TYPE_DECIMAL128>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromLargeIntToDecimal128p38s9Abnormal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "8687739038764787043432191044033164430", 38, 9, "", "true" },
    {-1, -1, "170141183460469231731687303715884105727", 38, 9, "", "true" },
    {-1, -1, "-2889410470202484480434731888456592258", 38, 9, "", "true" },
    {-1, -1, "-489439816566472801991842780194724", 38, 9, "", "true" },
    {-1, -1, "-4788595682559849290949209487144", 38, 9, "", "true" },
    {-1, -1, "-188739560553637417205545591395", 38, 9, "", "true" },
    {-1, -1, "-260581566657755975597344224111", 38, 9, "", "true" },
    {-1, -1, "-247047313389116644726645436646", 38, 9, "", "true" }
    };
    test_cast_all_fail<TYPE_LARGEINT, TYPE_DECIMAL128>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromLargeIntToDecimal128p38s9Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "35170199332856106292", 38, 9, "35170199332856106292", "false" },
    {-1, -1, "632624710680430242389045", 38, 9, "632624710680430242389045", "false" },
    {-1, -1, "187650181", 38, 9, "187650181", "false" },
    {-1, -1, "-276393546423", 38, 9, "-276393546423", "false" },
    {-1, -1, "-255823105130481504734607385", 38, 9, "-255823105130481504734607385", "false" },
    {-1, -1, "5903090217417102882055688784", 38, 9, "5903090217417102882055688784", "false" },
    {-1, -1, "-7184680", 38, 9, "-7184680", "false" },
    {-1, -1, "-808146102683417", 38, 9, "-808146102683417", "false" },
    {-1, -1, "26752", 38, 9, "26752", "false" },
    {-1, -1, "32794", 38, 9, "32794", "false" },
    {-1, -1, "60536924055682", 38, 9, "60536924055682", "false" },
    {-1, -1, "-106699900", 38, 9, "-106699900", "false" }
    };
    test_cast_all<TYPE_LARGEINT, TYPE_DECIMAL128>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromLargeIntToDecimal128p38s13Abnormal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "33094138259661154325431291530", 38, 13, "", "true" },
    {-1, -1, "2318161215485389246542634678", 38, 13, "", "true" },
    {-1, -1, "36109148148733805430655874797359", 38, 13, "", "true" },
    {-1, -1, "81177877972554250970903279475700", 38, 13, "", "true" },
    {-1, -1, "-493492240451920124932499981", 38, 13, "", "true" },
    {-1, -1, "-527756184261379113343583134216650371", 38, 13, "", "true" }
    };
    test_cast_all_fail<TYPE_LARGEINT, TYPE_DECIMAL128>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromLargeIntToDecimal128p38s13Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "4400188408329052789", 38, 13, "4400188408329052789", "false" },
    {-1, -1, "-18080830777", 38, 13, "-18080830777", "false" },
    {-1, -1, "652434855049", 38, 13, "652434855049", "false" },
    {-1, -1, "906394244", 38, 13, "906394244", "false" },
    {-1, -1, "252090410890457936", 38, 13, "252090410890457936", "false" },
    {-1, -1, "-5472418296196", 38, 13, "-5472418296196", "false" },
    {-1, -1, "15765388155744971589172079", 38, 13, "15765388155744971589172079", "false" },
    {-1, -1, "-224", 38, 13, "-224", "false" },
    {-1, -1, "382137160735", 38, 13, "382137160735", "false" },
    {-1, -1, "127", 38, 13, "127", "false" },
    {-1, -1, "-5432122119103", 38, 13, "-5432122119103", "false" },
    {-1, -1, "-22983274301466319054", 38, 13, "-22983274301466319054", "false" },
    {-1, -1, "591837589727708", 38, 13, "591837589727708", "false" },
    {-1, -1, "1739001", 38, 13, "1739001", "false" }
    };
    test_cast_all<TYPE_LARGEINT, TYPE_DECIMAL128>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromLargeIntToDecimal128p38s18Abnormal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "2422502809279453616961311332383206", 38, 18, "", "true" },
    {-1, -1, "250811768984805774405490069381", 38, 18, "", "true" },
    {-1, -1, "-110001182842337862243808755", 38, 18, "", "true" },
    {-1, -1, "-7798588819838971201590936683418193", 38, 18, "", "true" },
    {-1, -1, "4120867886031060104916532244", 38, 18, "", "true" },
    {-1, -1, "-180126121861933240329009894785", 38, 18, "", "true" }
    };
    test_cast_all_fail<TYPE_LARGEINT, TYPE_DECIMAL128>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromLargeIntToDecimal128p38s18Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "1", 38, 18, "1", "false" },
    {-1, -1, "32665268", 38, 18, "32665268", "false" },
    {-1, -1, "-4150723193691", 38, 18, "-4150723193691", "false" },
    {-1, -1, "-13872668481", 38, 18, "-13872668481", "false" },
    {-1, -1, "-99916514", 38, 18, "-99916514", "false" },
    {-1, -1, "118529641967", 38, 18, "118529641967", "false" },
    {-1, -1, "1061", 38, 18, "1061", "false" },
    {-1, -1, "161014653", 38, 18, "161014653", "false" },
    {-1, -1, "1949522", 38, 18, "1949522", "false" },
    {-1, -1, "-3", 38, 18, "-3", "false" },
    {-1, -1, "50468850", 38, 18, "50468850", "false" },
    {-1, -1, "-22962379514337937361", 38, 18, "-22962379514337937361", "false" },
    {-1, -1, "-112210796894555241", 38, 18, "-112210796894555241", "false" },
    {-1, -1, "-3078056", 38, 18, "-3078056", "false" }
    };
    test_cast_all<TYPE_LARGEINT, TYPE_DECIMAL128>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromLargeIntToDecimal128p35s30Abnormal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "-8952985878682445", 35, 30, "", "true" },
    {-1, -1, "-300056902586052242473989008215153297", 35, 30, "", "true" },
    {-1, -1, "7241499620753975041226772156823579", 35, 30, "", "true" },
    {-1, -1, "-37134896071808347443", 35, 30, "", "true" },
    {-1, -1, "-3556961353663961506369631403914012", 35, 30, "", "true" },
    {-1, -1, "1862055688121682", 35, 30, "", "true" },
    {-1, -1, "2739024111698593285168117902314391403", 35, 30, "", "true" },
    {-1, -1, "-604955383321326116786994400", 35, 30, "", "true" },
    {-1, -1, "3916730383358139793515144560922403", 35, 30, "", "true" },
    {-1, -1, "-805109900300749", 35, 30, "", "true" },
    {-1, -1, "1070140851421894503346937743695", 35, 30, "", "true" },
    {-1, -1, "60105814621251864402310401430108961028", 35, 30, "", "true" },
    {-1, -1, "-170141183460469231731687303715884105728", 35, 30, "", "true" },
    {-1, -1, "4377495216897350", 35, 30, "", "true" },
    {-1, -1, "1661959969314500293", 35, 30, "", "true" }
    };
    test_cast_all_fail<TYPE_LARGEINT, TYPE_DECIMAL128>(test_cases);
}

TEST_F(VectorizedDecimalCastExprIntegerTest, testCastFromLargeIntToDecimal128p35s30Normal) {
    CastTestCaseArray test_cases = {
    {-1, -1, "19102097", 35, 30, "19102097", "false" },
    {-1, -1, "-1427", 35, 30, "-1427", "false" },
    {-1, -1, "-7755", 35, 30, "-7755", "false" },
    {-1, -1, "1027", 35, 30, "1027", "false" },
    {-1, -1, "125917", 35, 30, "125917", "false" }
    };
    test_cast_all<TYPE_LARGEINT, TYPE_DECIMAL128>(test_cases);
}