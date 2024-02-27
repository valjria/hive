use movielens;

CREATE table if not exists movielens.ratings(
user_id int,
item_id int,
rating int,
rating_time bigint)
row format delimited
fields terminated by '\t'
lines terminated by '\n'
stored as textfile
tblproperties('skip.header.line.count'='1');

load data local inpath '/home/train/datasets/u.data' into table movielens.ratings ;

SELECT * FROM  ratings limit 10;

CREATE table if not exists movielens.movies(
movieid int,
movietitle string,
releasedate string,
videoreleasedate string,
IMDbURL string,
unknown int,
Action int,
Adventure int,
Animation int,
Childrens int,
Comedy int,
Crime int,
Documentary int,
Drama int,
Fantasy int,
FilmNoir int,
Horror int,
Musical int,
Mystery int,
Romance int,
SciFi int,
Thriller int,
War int,
Western int
)
row format delimited
fields terminated by '|'
lines terminated by '\n'
stored as textfile
tblproperties('skip.header.line.count'='1');



load data local inpath '/home/train/datasets/u.item' into table movielens.movies ;

SELECT * from movies limit 10;



--SELECT count(DISTINCT releasedate) FROM movies;

create table if not exists movielens.movie_ratings(
user_id int,
rating int,
rating_time bigint,
movieid int,
movietitle string,
videoreleasedate string,
imdburl string)
partitioned by (review_year int, review_month int)
clustered by (movietitle) into 4 buckets
stored as orc;

describe movie_ratings;

set hive.exec.dynamic.partition=TRUE ;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.enforce.bucketing=true;

insert overwrite table movielens.movie_ratings PARTITION(review_year, review_month)
select
user_id ,
rating ,
rating_time ,
movieid ,
movietitle ,
videoreleasedate,
imdburl ,
YEAR (from_unixtime(rating_time, 'yyyy-MM-dd')) as review_year,
MONTH (from_unixtime(rating_time, 'yyyy-MM-dd')) as review_month
from movielens.ratings r join movielens.movies m on r.item_id  = m.movieid ;

select count(*) from movie_ratings ;
show partitions movielens.movie_ratings;

describe movielens.movie_ratings;
select count (DISTINCT review_year, review_month ) from movie_ratings ;

select count(*) total_count, movietitle
from movielens.movie_ratings 
where review_year = 1998 and review_month = 4
group by movietitle order by total_count desc limit 20;

select avg(rating) as avg_rating, count(*) total_count, movietitle
from movielens.movie_ratings 
where review_year = 1998 and review_month = 4
group by movietitle order by avg_rating desc limit 20;


