--creates a spatial index on the species dataset by creating new geometries for each species that are the intersection of the species range with one degree cells. The resulting dataset contains the species id, the cell id, whether the intersection is completely covered by the cell, the intersection geometry and the area of the cell or the intersection geometry
SET mapreduce.map.java.opts=-Djava.net.preferIPv4Stack=true -Xmx1700m;
SET mapreduce.map.memory.mb=2048;
SET mapred.reduce.tasks=25;
DROP TABLE species.species_2014_2_unique;
CREATE TABLE species.species_2014_2_unique STORED AS SEQUENCEFILE AS SELECT id_no, ST_AsBinary(ST_Simplify(ST_Aggr_Union(ST_GeomFromWKB(shape)))) AS geom FROM species.species_2014_2 WHERE presence IN (1,2) AND origin IN (1,2) AND seasonal IN (1,2,3) AND id_no = 22725044 GROUP BY id_no;
SET mapreduce.map.java.opts=-Djava.net.preferIPv4Stack=true -Xmx825955249;
SET mapreduce.map.memory.mb=1024;
SET mapreduce.map.java.opts=-Djava.net.preferIPv4Stack=true -Xmx1700m; 
SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat; 
SET mapreduce.map.memory.mb=2048; 
SET mapred.map.tasks=1000; 
DROP TABLE species.species_2014_2_unique_index; 
CREATE TABLE species.species_2014_2_unique_index STORED AS SEQUENCEFILE AS SELECT id_no, cell, fully_covered, cell_intersect, IF(cell_intersect IS NULL, geotools_cell_area(cell, 1.0, "EPSG:4326", "EPSG:54009"), geotools_area(ST_Transform("EPSG:4326", "EPSG:54009", cell_intersect))) area FROM (SELECT id_no, c.cell, c.fully_covered, IF(fully_covered, NULL, geotools_cell_intersects(1.0, c.cell, geom)) cell_intersect FROM species.species_2014_2_unique LATERAL  VIEW esri_cells_udtf(1.0, geom) c) sub; 
SET hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat; 
SET mapreduce.map.java.opts=-Djava.net.preferIPv4Stack=true -Xmx825955249; 
SET mapreduce.map.memory.mb=1024; 
SET mapred.reduce.tasks=200;
DROP TABLE species.areatest1;
CREATE TABLE species.areatest1 
STORED AS SEQUENCEFILE AS SELECT s.id_no, w.wdpa_id, s.fully_covered species_fully_covered, w.fully_covered wdpa_fully_covered, s.area species_area, w.area wdpa_area, s.cell_intersect species_cell_intersect, w.cell_intersect wdpa_cell_intersect FROM tmp.wdpa_index w JOIN species.species_2014_2_unique_index s ON (s.cell = w.cell) WHERE s.fully_covered OR w.fully_covered OR ST_Intersects(ST_GeomFromWkb(s.cell_intersect), ST_GeomFromWkb(w.cell_intersect));
DROP TABLE species.species_2014_2_wdpa;
CREATE TABLE species.species_2014_2_wdpa 
STORED AS SEQUENCEFILE AS 
SELECT id_no, wdpa_id, SUM (CASE WHEN species_fully_covered AND wdpa_fully_covered THEN species_area WHEN species_fully_covered AND NOT wdpa_fully_covered THEN wdpa_area WHEN wdpa_fully_covered AND NOT species_fully_covered THEN species_area ELSE ST_Area(ST_Intersection(ST_GeomFromWKB(ST_Transform("EPSG:4326", "EPSG:54009", species_cell_intersect)), ST_GeomFromWKB(ST_Transform("EPSG:4326", "EPSG:54009", wdpa_cell_intersect)))) END / 1000000 ) total_area FROM species.areatest1 t GROUP BY id_no, wdpa_id;
DROP TABLE species.areatest1;
