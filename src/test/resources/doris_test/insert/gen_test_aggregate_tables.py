data = [0 for _ in range(3)]
for i in range(10):
    data[0] = str(i)
    data[1] = str(int(i / 10))
    data[2] = '"varchar' + str(int(i / 100)) + '"'

    print("\t".join(data))

# load
"""
curl --location-trusted -u root:123 -T ${3} -H "columns:k1, k2, k3, b1=to_bitmap(k1), b2=to_bitmap(k2), b3=to_bitmap(k1 + k2), h1=hll_hash(k1), # h2=hll_hash(k2), h3=hll_hash(k3), p1=percentile_hash(k1), p2=percentile_hash(k2)" http://localhost:9110/api/${1}/${2}/_stream_load
"""
