SET mapred.reduce.tasks=200;
DROP TABLE tmp.areatest1;
CREATE TABLE tmp.areatest1 
STORED AS SEQUENCEFILE AS 
SELECT 
  s.id_no, 
  w.wdpa_id, 
  s.fully_covered species_fully_covered, 
  w.fully_covered wdpa_fully_covered, 
  s.area species_area, 
  w.area wdpa_area, 
  s.cell_intersect species_cell_intersect, 
  w.cell_intersect wdpa_cell_intersect 
FROM 
  tmp.wdpa_index w 
  JOIN species.species_2014_2_cells s ON (s.cell = w.cell) 
WHERE 
  s.fully_covered 
  OR w.fully_covered 
  OR geotools_intersects(s.cell_intersect, w.cell_intersect);
