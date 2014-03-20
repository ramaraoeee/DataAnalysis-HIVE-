CREATE TABLE IF NOT EXISTS s_enron 
(eid STRING,
 ts STRING,
 fro STRING,
 to STRING,
 cc STRING,
 subject STRING,
 context STRING,
 pos STRING,
 neg STRING)
 COMMENT 'enron-senti'ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011' STORED AS TEXTFILE;

LOAD DATA INPATH '/user/hduser/input/enron.csv' OVERWRITE INTO TABLE enron;

create table pos_words(pword STRING) COMMENT 'POS_SENTIMENT' STORED AS TEXTFILE;

create table neg_words(nword STRING) COMMENT 'NEG_SENTIMENT' STORED AS TEXTFILE;

LOAD DATA INPATH '/user/hduser/input/neg.csv' OVERWRITE INTO TABLE neg_words;

LOAD DATA INPATH '/user/hduser/input/pos.csv' OVERWRITE INTO TABLE pos_words;

create table email_sentiment(eid STRING,score DOUBLE) COMMENT 'EMAIL_SENTIMENT' STORED AS TEXTFILE;

create table lexicon(word STRING,score DOUBLE) COMMENT 'WORD-SENTIMENT' STORED AS TEXTFILE LOCATION '/user/hduser/input/word-sentiment';

insert overwrite table email_sentiment select * from (
    select rel1.eid1,(rel1.pscore+rel2.nscore)/rel3.counter AS score from 
        (select e.eid AS eid1,count(*) AS pscore from 
            (select eid,eview.emwords AS ewords from enron LATERAL VIEW explode(split(upper(context),' ')) eview AS emwords) e 
            JOIN (select explode(split(pword,'  ')) AS plist from pos_words) pw ON (pw.plist=e.ewords) group by e.eid) rel1 
            JOIN (select e.eid AS eid2,(-1)*count(*) AS nscore from 
                (select eid,eview.emwords AS ewords from enron LATERAL VIEW explode(split(upper(context),' ')) eview AS emwords) e 
                JOIN (select explode(split(nword,'  ')) AS nlist from neg_words) nw ON (nw.nlist=e.ewords) group by e.eid) rel2 on (rel1.eid1=rel2.eid2) 
                JOIN (select eid AS eid3,count(emw1.ewords1) AS counter from enron LATERAL VIEW explode(split(upper(context),' ')) emw1 AS ewords1 group by eid) rel3 ON (rel3.eid3=rel1.eid1)) em4 
ORDER BY em4.score DESC;

insert overwrite table lexicon select e.eword2,e.sentiscore from 
    (select e1.eword1 AS eword2,sum(e2.score)/count(*) AS sentiscore from 
        (select eid,em.eword AS eword1 from enron LATERAL VIEW EXPLODE(SPLIT(UPPER(context),' ')) em AS eword) e1 
        JOIN (select eid,score from email_sentiment) e2 ON (e1.eid=e2.eid) GROUP BY e1.eword1) e 
order by e.sentiscore DESC;

insert overwrite table lexicon select rel1.word,rel1.counter from 
    (select pw.plist AS word,count(*) AS counter from (select explode(split(pword,'  ')) AS plist from pos_words) pw 
    JOIN (select eid,eview.emwords AS ewords from enron LATERAL VIEW explode(split(upper(context),' ')) eview AS emwords) e ON (e.ewords=pw.plist) 
    group by pw.plist) rel1 
order by rel1.counter DESC limit 30;

insert overwrite table lexicon select rel1.word,rel1.counter from 
    (select nw.nlist AS word,count(*) AS counter from 
        (select explode(split(nword,'  ')) AS nlist from neg_words) nw 
        JOIN (select eid,eview.emwords AS ewords from enron LATERAL VIEW explode(split(upper(context),' ')) eview AS emwords) e
         ON (e.ewords=nw.nlist) group by nw.nlist) rel1
 order by rel1.counter DESC limit 30;

add jar dataprocessing.jar;
create temporary function toyear as 'yearprocess.YearProcess';
create temporary function sentiment as 'sentiscore.emailsentiment';

select sent.id,sent.year,sent.score from (select e.eid AS id,e.yr AS year,sentiment(e.context,pos_words.pword,neg_words.nword) AS score from (select eid,toyear(ts) AS yr,fro,to,cc,subject,context from enron) e CROSS JOIN pos_words CROSS JOIN neg_words);

