-- Create point datastructure for easier geoqueries
ALTER TABLE `location` ADD COLUMN `loc` POINT AFTER latitude;
UPDATE `location` SET `loc` = POINT(longitude, latitude);
ALTER TABLE `important_locations` ADD COLUMN loc POINT AFTER latitude;
UPDATE `important_locations` SET loc = POINT(longitude, latitude);

-- Add indexes
ALTER TABLE `location` ADD INDEX `datetime_index` (`datetime` ASC);

-- Classify each coordinate based on the important locations dataset
ALTER TABLE `location` ADD COLUMN `name` TEXT;
UPDATE `location`
LEFT JOIN `important_locations`
	ON st_distance_sphere(location.loc, important_locations.loc) < important_locations.radius
SET location.name = important_locations.name;
