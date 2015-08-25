 /*Contenido mas visto		cd /user/hduser/netflix/*/
highview1 = load 'netflixMoviesValidosSimple3.csv' using PigStorage('|') as (idmovie:int, title:chararray, iduser:int, ranking:int, date:chararray, yearmovie:chararray);
highview2 = cogroup highview1 by title;
highview3 = foreach highview2 generate group , COUNT(highview1.iduser) as veces;
highview4 = order highview3 by veces desc;
highview5 = limit highview4 15;
store highview5 into 'highview' using PigStorage('|');


