--creates the union of the species range data for all species that have presence IN (1,2) AND origin IN (1,2) AND seasonal IN (1,2,3) and writes the output to a new table
SET mapreduce.map.java.opts=-Djava.net.preferIPv4Stack=true -Xmx1700m;
SET mapreduce.map.memory.mb=2048;
SET mapred.reduce.tasks=25;
DROP TABLE species.species_unique_2014_2;
CREATE TABLE species.species_unique_2014_2 STORED AS SEQUENCEFILE AS SELECT id_no, ST_AsBinary(ST_Simplify(ST_Aggr_Union(ST_GeomFromWKB(shape)))) AS geom FROM species.species_2014_2 WHERE presence IN (1,2) AND origin IN (1,2) AND seasonal IN (1,2,3) AND id_no = 17975 GROUP BY id_no;
SET mapreduce.map.java.opts=-Djava.net.preferIPv4Stack=true -Xmx825955249;
SET mapreduce.map.memory.mb=1024;