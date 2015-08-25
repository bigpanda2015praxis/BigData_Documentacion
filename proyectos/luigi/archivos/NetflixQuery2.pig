/*cd /user/hduser/netflix/*/
lowview1 = load 'netflixMoviesValidosSimple3.csv' using PigStorage('|') as (idmovie:int, title:chararray, iduser:int, ranking:int, date:chararray, yearmovie:chararray);
lowview2 = cogroup lowview1 by title;
lowview3 = foreach lowview2 generate group , COUNT(lowview1.iduser) as veces;
lowview4 = order lowview3 by veces asc;
lowview5 = limit lowview4 15;
store lowview5 into 'lowview' using PigStorage('|');
