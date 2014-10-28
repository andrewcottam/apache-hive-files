--creates a spatial index on the species dataset by creating new geometries for each species that are the intersection of the species range with one degree cells. The resulting dataset contains the species id, the cell id, whether the intersection is completely covered by the cell, the intersection geometry and the area of the cell or the intersection geometry
SET mapreduce.map.java.opts=-Djava.net.preferIPv4Stack=true -Xmx1700m;
SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SET mapreduce.map.memory.mb=2048;
SET mapred.map.tasks=1000;

DROP TABLE species.species_index;
CREATE TABLE species.species_index
STORED AS RCFILE
AS
SELECT
  objectid,
  id_no, 
  cell, 
  fully_covered, 
  cell_intersect,
  IF(cell_intersect IS NULL, geotools_cell_area(cell, 1.0, "EPSG:4326", "EPSG:54009"), geotools_area(ST_Transform("EPSG:4326", "EPSG:54009", cell_intersect))) area
FROM (
  SELECT 
    objectid,
    id_no, 
	c.cell, 
	c.fully_covered, 
	IF(fully_covered, NULL, cell_intersects(1.0, c.cell, geom)) cell_intersect
  FROM 
    species.species_2014_2 LATERAL VIEW esri_cells_udtf(1.0, geom) c
  ) sub;

SET hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET mapreduce.map.java.opts=-Djava.net.preferIPv4Stack=true -Xmx825955249;
SET mapreduce.map.memory.mb=1024;

