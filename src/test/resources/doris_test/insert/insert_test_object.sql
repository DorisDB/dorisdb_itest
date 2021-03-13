insert into test_object (v1, v2, v3, v4, b1, b2, b3, b4, h1, h2, h3, h4) values
    (12, 10, 10, 10000, to_bitmap(12), to_bitmap(10), to_bitmap(100), to_bitmap(10000), hll_hash(12), hll_hash(10), hll_hash(100), hll_hash(10000)),
    (22, 20, 20, 20000, to_bitmap(22), to_bitmap(20), to_bitmap(200), to_bitmap(20000), hll_hash(22), hll_hash(20), hll_hash(200), hll_hash(20000)),
    (32, 30, 30, 30000, to_bitmap(32), to_bitmap(30), to_bitmap(300), to_bitmap(30000), hll_hash(32), hll_hash(30), hll_hash(300), hll_hash(30000)),
    (42, 40, 40, 20000, to_bitmap(42), to_bitmap(40), to_bitmap(400), to_bitmap(20000), hll_hash(42), hll_hash(40), hll_hash(400), hll_hash(20000)),
    (52, 50, 10, 10000, to_bitmap(52), to_bitmap(50), to_bitmap(100), to_bitmap(10000), hll_hash(52), hll_hash(50), hll_hash(100), hll_hash(10000)),
    (62, 60, 20, 20000, to_bitmap(62), to_bitmap(60), to_bitmap(200), to_bitmap(20000), hll_hash(62), hll_hash(60), hll_hash(200), hll_hash(20000)),
    (72, 70, 30, 30000, to_bitmap(72), to_bitmap(70), to_bitmap(300), to_bitmap(30000), hll_hash(72), hll_hash(70), hll_hash(300), hll_hash(30000)),
    (82, 20, 10, 40000, to_bitmap(82), to_bitmap(20), to_bitmap(100), to_bitmap(40000), hll_hash(82), hll_hash(20), hll_hash(100), hll_hash(40000)),
    (92, 80, 10, 00000, to_bitmap(92), to_bitmap(80), to_bitmap(100), to_bitmap(00000), hll_hash(92), hll_hash(80), hll_hash(100), hll_hash(00000)),
    (10, 10, 10, 10000, to_bitmap(10), to_bitmap(10), to_bitmap(100), to_bitmap(10000), hll_hash(10), hll_hash(10), hll_hash(100), hll_hash(10000)),
    (11, 70, 20, 20000, to_bitmap(11), to_bitmap(70), to_bitmap(200), to_bitmap(20000), hll_hash(11), hll_hash(70), hll_hash(200), hll_hash(20000)),
    (12, 80, 40, 99000, to_bitmap(12), to_bitmap(80), to_bitmap(400), to_bitmap(99000), hll_hash(12), hll_hash(80), hll_hash(400), hll_hash(99000)),
    (13, 90, 80, 20000, to_bitmap(13), to_bitmap(90), to_bitmap(800), to_bitmap(20000), hll_hash(13), hll_hash(90), hll_hash(800), hll_hash(20000));
