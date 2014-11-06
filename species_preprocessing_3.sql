--first stage in the species/wdpa processing to get the areas of intersection
SET mapred.reduce.tasks=200;
DROP TABLE species.areatest1;
CREATE TABLE species.areatest1 
STORED AS SEQUENCEFILE AS SELECT s.id_no, w.wdpa_id, s.fully_covered species_fully_covered, w.fully_covered wdpa_fully_covered, s.area species_area, w.area wdpa_area, s.cell_intersect species_cell_intersect, w.cell_intersect wdpa_cell_intersect FROM tmp.wdpa_index w JOIN species.species_unique_2014_2_index s ON (s.cell = w.cell) WHERE s.fully_covered OR w.fully_covered OR ST_Intersects(ST_GeomFromWkb(s.cell_intersect), ST_GeomFromWkb(w.cell_intersect));
