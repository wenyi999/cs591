import assignment3

def test_distinct():
    scan_data=Scan('../data/sample.csv',None,False,False)
    distinct=Distinct(scan_data,7,False,False)
    ans=distinct.get_next()
    assert ans["empty"]==5114

def test_map():
    scan_data=Scan('../data/sample.csv',None,False,False)
    map_data=Map(scan_data,keys)
    data_ETL=map_data.get_next()
    assert data_ETL[0]==[1, 15, 11, 6, 2511988, 228751, 33638, 47079934, 0, 0, 0, 0]
    


