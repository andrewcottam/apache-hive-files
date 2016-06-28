SET hive.exec.compress.intermediate=true;
SET hive.exec.compress.output=true;
SET avro.output.codec=snappy;

-- CREATE THE SPECIES AND WDPA TABLES
CREATE EXTERNAL TABLE IF NOT EXISTS default.species_borneo
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS
  INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '/user/cottaan/data/species_borneo'
TBLPROPERTIES ('avro.schema.url'='hdfs:////user/cottaan/schema/species_borneo.avsc');

CREATE EXTERNAL TABLE IF NOT EXISTS default.wdpa_borneo
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS
  INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '/user/cottaan/data/wdpa_borneo'
TBLPROPERTIES ('avro.schema.url'='hdfs:////user/cottaan/schema/wdpa_borneo.avsc');

-- DISSOLVE THE SPECIES GEOMETRIES ON THE SPECIES ID FIELD
SET mapreduce.map.java.opts=-Djava.net.preferIPv4Stack=true -Xmx1700m; --Larger heap-size for child jvms of maps
SET mapreduce.map.memory.mb=2048; --Larger resource limit for maps
SET mapred.reduce.tasks=25; --Number of reducers
DROP TABLE IF EXISTS species_borneo_dissolved;
CREATE TABLE species_borneo_dissolved STORED AS SEQUENCEFILE AS SELECT id_no, ST_AsBinary(ST_Simplify(ST_Aggr_Union(ST_GeomFromWKB(shape)))) AS geom FROM species_borneo WHERE presence IN (1,2) AND origin IN (1,2) AND seasonal IN (1,2,3) GROUP BY id_no;

--BUILD THE SPATIAL INDICES
SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat; 
SET mapred.map.tasks=1000; 
DROP TABLE IF EXISTS species_borneo_dissolved_index;
CREATE TABLE species_borneo_dissolved_index STORED AS SEQUENCEFILE AS SELECT id_no, cell, fully_covered, cell_intersect, IF(cell_intersect IS NULL, geotools_cell_area(cell, 1.0, "EPSG:4326", "EPSG:54009"), geotools_area(ST_Transform("EPSG:4326", "EPSG:54009", cell_intersect))) area FROM (SELECT id_no, c.cell, c.fully_covered, IF(fully_covered, NULL, geotools_cell_intersects(1.0, c.cell, geom)) cell_intersect FROM species_borneo_dissolved LATERAL VIEW esri_cells_udtf(1.0, geom) c) sub;
DROP TABLE IF EXISTS wdpa_borneo_index;
CREATE TABLE wdpa_borneo_index STORED AS SEQUENCEFILE AS SELECT wdpa_id, cell, fully_covered, cell_intersect, IF(cell_intersect IS NULL, geotools_cell_area(cell, 1.0, "EPSG:4326", "EPSG:54009"), geotools_area(ST_Transform("EPSG:4326", "EPSG:54009", cell_intersect))) area FROM (SELECT wdpa_id, c.cell, c.fully_covered, IF(fully_covered, NULL, geotools_cell_intersects(1.0, c.cell, geom)) cell_intersect FROM wdpa_borneo LATERAL VIEW esri_cells_udtf(1.0, geom) c) sub;
SET hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat; 
SET mapreduce.map.java.opts=-Djava.net.preferIPv4Stack=true -Xmx825955249; 
SET mapreduce.map.memory.mb=1024; 

--DO THE QUERY TO GET THE AREAS OF INTERSECTION
SET mapred.reduce.tasks=200;
DROP TABLE IF EXISTS species_wdpa_areas;
CREATE TABLE species_wdpa_areas STORED AS SEQUENCEFILE AS SELECT s.id_no, w.wdpa_id, s.fully_covered species_fully_covered, w.fully_covered wdpa_fully_covered, s.area species_area, w.area wdpa_area, s.cell_intersect species_cell_intersect, w.cell_intersect wdpa_cell_intersect FROM wdpa_borneo_index w JOIN species_borneo_dissolved_index s ON (s.cell = w.cell) WHERE s.fully_covered OR w.fully_covered OR ST_Intersects(ST_GeomFromWkb(s.cell_intersect), ST_GeomFromWkb(w.cell_intersect));
DROP TABLE IF EXISTS species_wdpa_areas_summed;
CREATE TABLE species_wdpa_areas_summed STORED AS SEQUENCEFILE AS SELECT id_no, wdpa_id, SUM (CASE WHEN species_fully_covered AND wdpa_fully_covered THEN species_area WHEN species_fully_covered AND NOT wdpa_fully_covered THEN wdpa_area WHEN wdpa_fully_covered AND NOT species_fully_covered THEN species_area ELSE ST_Area(ST_Intersection(ST_GeomFromWKB(ST_Transform("EPSG:4326", "EPSG:54009", species_cell_intersect)), ST_GeomFromWKB(ST_Transform("EPSG:4326", "EPSG:54009", wdpa_cell_intersect)))) END / 1000000 ) total_area FROM species_wdpa_areas t GROUP BY id_no, wdpa_id;