--second stage in the species/wdpa processing to get the areas of intersection
DROP TABLE species.areatest2;
CREATE TABLE species.areatest2 
STORED AS SEQUENCEFILE AS 
SELECT 
	id_no, 
	wdpa_id, 
	SUM ( CASE 
		WHEN species_fully_covered AND wdpa_fully_covered THEN species_area 
		WHEN species_fully_covered AND NOT wdpa_fully_covered THEN wdpa_area 
		WHEN wdpa_fully_covered AND NOT species_fully_covered THEN species_area 
		ELSE ST_Area(ST_Intersection(ST_GeomFromWKB(ST_Transform("EPSG:4326", "EPSG:54009", species_cell_intersect)), ST_GeomFromWKB(ST_Transform("EPSG:4326", "EPSG:54009", wdpa_cell_intersect)))) 
	END / 1000000 ) total_area 
FROM 
	species.areatest1 t 
GROUP BY id_no, wdpa_id;