import assignment_12

def test_scan():
    filter_friend=Filter(1190,0)
    scan_friends=Scan("../data/friends.txt",filter_friend,False,False)
    assert scan_friend.get_next()[0].tuple=="1190 15"

def test_join():
    filter_friend=Filter(1190,0)
    scan_friends=Scan("../data/friends.txt",filter_friend,False,False)

    filter_movie=Filter(15,1)
    scan_movies=Scan("../data/movie_ratings.txt",filter_movie,False,False)

    join_opt=Join(scan_friends,scan_movies,1,0,False,False)
    assert join_opt.get_next()[0].tuple=="1190 15 15 15 2"

def test_avg():
    agg_fun=AggFun()
    AT1=ATuple("11 2")
    AT2=ATuple("-1 3")
    assert agg_fun.AVG([AT1,AT2],1)==2.5

def test_project():
    filter_friend=Filter(1190,0)
    scan_friends=Scan("../data/friends.txt",filter_friend,False,False)

    projection=Project(scan_friends,[0],False,False)
    assert projection.get_next()[0].tuple=="1190"

def test_group_by():
    filter_friend=Filter(1190,0)
    scan_friends=Scan("../data/friends.txt",filter_friend,False,False)

    group_by_opt=GroupBy(scan_friends,0,1,average,False,False)
    assert group_by_opt.get_next()[0].tuple=="1190 890.5365853658536"

def test_order_by():
    filter_friend=Filter(1190,0)
    scan_friends=Scan("../data/friends.txt",filter_friend,False,False)

    compare_opt=Comparator(1)
    order_by_opt=OrderBy(scan_friends,compare_opt,False,False,False)
    assert order_by_opt.get_next()[0].tuple=="1190 1974"

def test_topk():
    filter_friend=Filter(1190,0)
    scan_friends=Scan("../data/friends.txt",filter_friend,False,False)

    top_k_opt=TopK(scan_friends,1,False,False)
    assert top_k_opt.get_next()[0].tuple=="1190 15"

def test_hist():
    filter_friend=Filter(1190,0)
    scan_friends=Scan("../data/friends.txt",filter_friend,False,False)
    filter_movie=Filter(15,1)
    scan_movies=Scan("../data/movie_ratings.txt",filter_movie,False,False)
    join_opt=Join(scan_friends,scan_movies,1,0,False,False)
    hist_opt=Histogram(join_opt,4,False,False)
    ans3=hist_opt.get_next()
    assert ans3=={'2 points': 6, '4 points': 8, '1 points': 4, '0 points': 13, '3 points': 4, '5 points': 6}


