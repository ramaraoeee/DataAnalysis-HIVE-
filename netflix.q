create table movie_ratings (SNO INT, CUSTID INT, RATING INT, DATE STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' lines terminated by '\n' LOCATION 's3://spring-2014-ds/movie_dataset/movie_ratings/';

create table ratings100(sno INT,custid INT,ratings INT) STORED AS TEXTFILE LOCATION 's3://dsciproj/output/ratings100/';

insert overwrite TABLE ratings100 select m3.sno1,m3.custid1,m3.rating1 from (select m2.sno AS sno1,m1.custid1 AS custid1,m2.rating AS rating1,m1.counter from (select m2.custid AS custid1,m2.counter from (select custid,count(*) AS counter from movie_ratings group by custid) m2 order by m2.counter DESC LIMIT 100) m1 JOIN movie_ratings m2 ON (m1.custid1=m2.custid)) m3;

create table corr_res(user1 int,user2 int,score DOUBLE) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011' lines terminated by '\n' STORED AS TEXTFILE LOCATION 's3://dsciproj/output/corr_res';

insert overwrite table corr_res select cu1.cid AS cust1,cu2.cid AS cust2,covar_samp(cu1.score,cu2.score)/(cu1.dev1*cu2.dev1) FROM (Select r1.custid AS cid,r1.av AS average,r2.sno AS mid,r2.ratings AS score,r1.devsum as dev1 from (select custid,avg(ratings) AS av,stddev_samp(ratings) AS devsum from ratings100 group by custid) r1 JOIN ratings100 r2 ON (r1.custid=r2.custid)) cu1 JOIN (Select r1.custid AS cid,r1.av AS average,r2.sno AS mid,r2.ratings AS score,r1.devsum AS dev1 from (select custid,avg(ratings) AS av,stddev_samp(ratings) AS devsum from ratings100 group by custid) r1 JOIN ratings100 r2 ON (r1.custid=r2.custid)) cu2 ON (cu1.mid=cu2.mid) group by cu1.cid,cu2.cid,cu1.dev1,cu2.dev1;

CREATE EXTERNAL TABLE suggestmovies ( MOVIE STRING, RATING FLOAT ) row format delimited fields terminated by ',' lines terminated by '\n' STORED AS TEXTFILE LOCATION 's3://dscience/q1/';

INSERT OVERWRITE TABLE suggestmovies Select   m21.MOVIE, m20.ranking from  (select m5.sno, count(m5.sno) AS ranking from (select m4.sno,m4.rating from (select m1.CUSTID from (select CUSTID , SNO from movie_ratings where RATING >=4 AND custid!= 852256)m1 JOIN (SELECT SNO, RATING FROM movie_ratings m2 where custid = 852256 ORDER BY RATING DESC LIMIT 20) m2 ON (m1.SNO=m2.SNO)  Group by  m1.CUSTID)m3  JOIN ( select m10.sno,m10.rating,m10.custid from (select custid,sno,rating from movie_ratings)m10 LEFT OUTER  JOIN (select custid,sno  from movie_ratings where custid =852256)m11 on (m10.sno=m11.sno) where m11.sno is null AND m10.RATING =5)m4 ON (m3.custid = m4.custid))m5 group by m5.sno ORDER BY ranking DESC LIMIT 20)m20 JOIN (select MOVIE,SNO from movie_titles )m21 on (m21.sno = m20.sno);

CREATE EXTERNAL TABLE suggestmovies ( MOVIE STRING, RATING FLOAT ) row format delimited fields terminated by ',' lines terminated by '\n' STORED AS TEXTFILE LOCATION 's3://dscience/q1/';

INSERT OVERWRITE TABLE suggestmovies Select   m21.MOVIE, m20.ranking from  (select m5.sno, count(m5.sno) AS ranking from (select m4.sno,m4.rating from (select m1.CUSTID from (select CUSTID , SNO from movie_ratings where RATING >=4 AND custid!= 852256)m1 JOIN (SELECT SNO, RATING FROM movie_ratings m2 where custid = 852256 ORDER BY RATING DESC LIMIT 20) m2 ON (m1.SNO=m2.SNO)  Group by  m1.CUSTID)m3  JOIN ( select m10.sno,m10.rating,m10.custid from (select custid,sno,rating from movie_ratings)m10 LEFT OUTER  JOIN (select custid,sno  from movie_ratings where custid =852256)m11 on (m10.sno=m11.sno) where m11.sno is null AND m10.RATING =5)m4 ON (m3.custid = m4.custid))m5 group by m5.sno ORDER BY ranking DESC LIMIT 20)m20 JOIN (select MOVIE,SNO from movie_titles )m21 on (m21.sno = m20.sno);

CREATE EXTERNAL TABLE suggestfriends ( CUSTID INT ) row format delimited fields terminated by ',' lines terminated by '\n' STORED AS TEXTFILE LOCATION 's3://dscience/q3/';

INSERT OVERWRITE TABLE suggestfriends select m1.CUSTID, sum(m1.rating)/5 as ranking from (select CUSTID , SNO, Rating from movie_ratings where RATING =5 AND custid!= 852256 )m1 JOIN (SELECT SNO, RATING FROM movie_ratings where custid= 852256 ORDER BY RATING DESC LIMIT 500) m2 ON (m1.SNO=m2.SNO) group by m1.custid ORDER BY ranking DESC LIMIT 50;

CREATE EXTERNAL TABLE decadelist ( DECADE INT, NUM_MOVIES INT , RATING FLOAT ) row format delimited fields terminated by ',' lines terminated by '\n' STORED AS TEXTFILE LOCATION 's3://dscience/q4/';

INSERT OVERWRITE  TABLE decadelist   select Floor(M1.Year/10)*10, count(M1.Movie), Avg(M2.Rating) from movie_titles M1 JOIN movie_ratings M2 on (M1.sno = M2.sno) group by Floor(M1.Year/10);


