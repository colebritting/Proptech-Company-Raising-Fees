--Creating table for old data

CREATE TABLE old_pma (
	owner_id VARCHAR(30),
	owner_onboarded_date DATE,
	property_id VARCHAR(30) PRIMARY KEY,
	managed_since DATE,
	normal_units_count SMALLINT,
	pm_fee_percent VARCHAR(5),
	pm_fee_per_property DECIMAL(8,2),
	pm_fee_per_unit DECIMAL(8,2),
	lease_renewal_fee_value DECIMAL(8,2), 
	lease_renewal_fee_type VARCHAR(15),
	leasing_fee_value DECIMAL SMALLINT, 
	leasing_fee_type VARCHAR(15),
	msa VARCHAR(30),
	region_name VARCHAR(30),
	hub_name VARCHAR(30),
	rent_total DECIMAL(8,2)
);

--Copying data into table

COPY old_pma 
FROM 'C:\Users\Owner\Downloads\Job Portfolio\PropTech Project\old_pma.csv'
DELIMITER ','
CSV HEADER;

--Noticing error where I have integer in some columns where decimal is needed

ALTER TABLE old_pma 
ALTER COLUMN leasing_fee_value TYPE DECIMAL(8,2);

-- Copying data in

COPY old_pma 
FROM 'C:\Users\Owner\Downloads\Job Portfolio\PropTech Project'
DELIMITER ','
CSV HEADER;

--Another error, one of the data points in the date column is not in proper format. Making it varchar now, will change later

ALTER TABLE old_pma 
ALTER COLUMN owner_onboarded_date TYPE VARCAHR(20);

ALTER TABLE old_pma 
ALTER COLUMN managed_since TYPE VARCAHR(20);

--Copy

COPY old_pma 
FROM 'C:\Users\Owner\Downloads\Job Portfolio\PropTech Project'
DELIMITER ','
CSV HEADER;

--Looking at data to make sure it worked properly

SELECT * FROM old_pma
LIMIT 5;

--Finding rows with improper date formats

SELECT owner_onboarded_date FROM old_pma
ORDER BY owner_onboarded_date;

--Found one row, looking at it

SELECT * FROM old_pma
WHERE owner_onboarded_date = 'Yes';

--Row is just completely misformatted with unreadable data, deleting it

DELETE FROM old_pma
WHERE owner_onboarded_date = 'Yes';

--Converting columns to date 

ALTER TABLE old_pma
ALTER COLUMN owner_onboarded_date TYPE DATE;

ALTER TABLE old_pma
ALTER COLUMN managed_since TYPE DATE;

--Error saying it cannot be cast automatically

ALTER TABLE old_pma
ALTER COLUMN owner_onboarded_date TYPE DATE USING ("owner_onboarded_date"::text::date);

ALTER TABLE old_pma
ALTER COLUMN managed_since TYPE DATE USING ("owner_onboarded_date"::text::date);

--Checking to see if it worked

SELECT * FROM old_pma
LIMIT 3;

--Converting a column with percent symbol into an int

UPDATE old_pma
SET pm_fee_percent = REPLACE(pm_fee_percent,'%','');

ALTER TABLE old_pma
ALTER COLUMN pm_fee_percent TYPE DECIMAL(6,2) USING ("pm_fee_percent"::NUMERIC(6,2));

UPDATE old_pma
SET pm_fee_percent = pm_fee_percent/100;

SELECT * FROM old_pma
LIMIT 3;

--Checking for null values in two key columns

SELECT * FROM old_pma
WHERE lease_renewal_fee_type IS NULL OR leasing_fee_type IS NULL;

--Only null where there is no fee value so this will not affect us
--Splitting msa into msa and state

ALTER TABLE old_pma
ADD COLUMN state;

UPDATE old_pma
SET state = SPLIT_PART(msa, ',', 2);

UPDATE old_pma
SET msa = SPLIT_PART(msa, ',', 1);

--Finding the old pm cost for each property

SELECT (12*((pm_fee_percent * rent_total) + 
(pm_fee_per_unit * normal_units_count) + pm_fee_per_property))
FROM old_pma
WHERE rent_total > 0;

ALTER TABLE old_pma
ADD COLUMN total_old_pm_fees DECIMAL(8,2);

UPDATE old_pma
SET total_old_pm_fees = (12*((pm_fee_percent * rent_total) + 
(pm_fee_per_unit * normal_units_count) + pm_fee_per_property))
WHERE rent_total > 0;

UPDATE old_pma
SET total_old_pm_fees = 0 
WHERE rent_total = 0;

SELECT * FROM old_pma 
LIMIT 5;

--Find old renewal leasing fees for current rent collected units

SELECT DISTINCT(lease_renewal_fee_type) 
FROM old_pma;

SELECT lease_renewal_fee_value * rent_total 
FROM old_pma
WHERE lease_renewal_fee_type = 'Percent' AND rent_total > 0;

SELECT lease_renewal_fee_value
FROM old_pma
WHERE lease_renewal_fee_type = 'Flat' AND rent_total > 0;

ALTER TABLE old_pma
ADD COLUMN renewal_fee DECIMAL(8,2);

UPDATE old_pma
SET renewal_fee = lease_renewal_fee_value * rent_total 
WHERE lease_renewal_fee_type = 'Percent' AND rent_total > 0;

UPDATE old_pma
SET renewal_fee = lease_renewal_fee_value 
WHERE lease_renewal_fee_type = 'Flat' AND rent_total > 0;

SELECT * FROM old_pma
--WHERE leasing_new_tenant_fee IS NULL
LIMIT 20;

UPDATE old_pma
SET renewal_fee = COALESCE(renewal_fee, 0);

--Now finding fees for new tenants to lease

SELECT DISTINCT(leasing_fee_type) 
FROM old_pma;

--Want to keep same formatting

UPDATE old_pma
SET leasing_fee_type = 'Flat'
WHERE leasing_fee_type = 'Amount';

SELECT DISTINCT(leasing_fee_type) 
FROM old_pma;

--Continuing to find leasing fees

SELECT leasing_fee_value
FROM old_pma
WHERE leasing_fee_type = 'Flat' AND rent_total > 0;

SELECT leasing_fee_value * rent_total
FROM old_pma
WHERE leasing_fee_type = 'Percent' AND rent_total > 0;

ALTER TABLE old_pma
ADD COLUMN leasing_new_tenant_fee DECIMAL(8,2);

UPDATE old_pma
SET leasing_new_tenant_fee = leasing_fee_value
WHERE leasing_fee_type = 'Flat' AND rent_total > 0;

UPDATE old_pma
SET leasing_new_tenant_fee = leasing_fee_value * rent_total
WHERE leasing_fee_type = 'Percent' AND rent_total > 0;

SELECT * FROM old_pma
--WHERE leasing_new_tenant_fee IS NULL
LIMIT 20;

UPDATE old_pma
SET leasing_new_tenant_fee = COALESCE(leasing_new_tenant_fee, 0);

--Denoting how many units the owner of each property has

SELECT SUM(normal_units_count) OVER(PARTITION BY owner_id) 
FROM old_pma;

ALTER TABLE old_pma
ADD COLUMN owner_units SMALLINT;

UPDATE old_pma
SET owner_units = SUM(normal_units_count) OVER(PARTITION BY owner_id) ;

--Forgot you cannot update with window functions, going to create a view with new info (using cte)


CREATE VIEW old_numbers AS (
WITH owner_units_cte AS (
	SELECT property_id, SUM(normal_units_count) OVER(PARTITION BY owner_id) as owner_units
	FROM old_pma
)
SELECT owner_id, owner_onboarded_date, old_pma.property_id,managed_since,normal_units_count,
msa, region_name, hub_name, rent_total, total_old_pm_fees, renewal_fee as old_renewal_fee,
leasing_new_tenant_fee as old_leasing_fee, owner_units FROM old_pma
JOIN owner_units_cte
ON owner_units_cte.property_id = old_pma.property_id);

SELECT * FROM old_numbers
LIMIT 5;

--Creating new table with new pricing values

CREATE TABLE new_prices(
	
	market VARCHAR(30),
	one_home SMALLINT,
	two_homes SMALLINT,
	three_homes SMALLINT,
	leasing_fee VARCHAR(10),
	renewal_fee SMALLINT
);

--Copying data in

COPY new_prices
FROM 'C:\Users\Owner\Downloads\Job Portfolio\PropTech Project\new_pricing.csv'
DELIMITER ','
CSV HEADER;

--Checking to see everything is fine

SELECT * FROM new_prices;

--Updating % column to take out symbol and make numeric

UPDATE new_prices
SET leasing_fee = REPLACE(leasing_fee,'%','');

ALTER TABLE new_prices
ALTER COLUMN leasing_fee TYPE DECIMAL(6,2) USING ("leasing_fee"::NUMERIC(6,2));

UPDATE new_prices
SET leasing_fee = leasing_fee/100;

SELECT * FROM new_prices
LIMIT 5;

--Joining the two tables to see if our market and msa match

SELECT DISTINCT(msa), market FROM old_numbers
LEFT JOIN new_prices
ON msa = market;

--Noticing that we have MSA values in our old numbers that don't match with something in current pricing
--This is a common occurence with Proptech/RE data - market names may not match up.
--Now we have to find a matching new price value for each old msa

SELECT msa FROM(
SELECT DISTINCT(msa), market FROM old_numbers
LEFT JOIN new_prices
ON msa = market) as a
WHERE market IS NULL

--  Vallejo, Boulder, Los Angeles, Hickory, Bremerton, San Jose, Deltona, Athens, Burlington, Granbury, Carson City, 
--  Bonham, Gainesville, Longview, Shelby, Cedartown, Salem, North Port, Lakeland, Macon, Santa Rosa, Winston, Palm Bay, 
--  Portland, Colorado Spings, Sanford, Dallas, Durham, Rome, Fernley, Jefferson, Killeen, San Francisco, Santa Cruz

--Find data points in our new pricing that include the cities above, and then create the rows to match perfectly

SELECT * FROM new_prices;

INSERT INTO new_prices (
	market, one_home, two_homes, three_homes, leasing_fee, renewal_fee)
	SELECT 'Greeley', one_home, two_homes, three_homes, leasing_fee, renewal_fee FROM new_prices
	WHERE market = 'Denver';
INSERT INTO new_prices (
	market, one_home, two_homes, three_homes, leasing_fee, renewal_fee)
	SELECT 'Vallejo', one_home, two_homes, three_homes, leasing_fee, renewal_fee FROM new_prices
	WHERE market = 'The Bay';
INSERT INTO new_prices (
	market, one_home, two_homes, three_homes, leasing_fee, renewal_fee)
	SELECT 'Boulder', one_home, two_homes, three_homes, leasing_fee, renewal_fee FROM new_prices
	WHERE market = 'Denver';
INSERT INTO new_prices (
	market, one_home, two_homes, three_homes, leasing_fee, renewal_fee)
	SELECT 'Los Angeles', one_home, two_homes, three_homes, leasing_fee, renewal_fee FROM new_prices
	WHERE market = 'SoCal';
INSERT INTO new_prices (
	market, one_home, two_homes, three_homes, leasing_fee, renewal_fee)
	SELECT 'Hickory', one_home, two_homes, three_homes, leasing_fee, renewal_fee FROM new_prices
	WHERE market = 'Charlotte';
INSERT INTO new_prices (
	market, one_home, two_homes, three_homes, leasing_fee, renewal_fee)
	SELECT 'Bremerton', one_home, two_homes, three_homes, leasing_fee, renewal_fee FROM new_prices
	WHERE market = 'Seattle';
INSERT INTO new_prices (
	market, one_home, two_homes, three_homes, leasing_fee, renewal_fee)
	SELECT 'San Jose', one_home, two_homes, three_homes, leasing_fee, renewal_fee FROM new_prices
	WHERE market = 'SoCal';
INSERT INTO new_prices (
	market, one_home, two_homes, three_homes, leasing_fee, renewal_fee)
	SELECT 'Deltona', one_home, two_homes, three_homes, leasing_fee, renewal_fee FROM new_prices
	WHERE market = 'Orlando';
INSERT INTO new_prices (
	market, one_home, two_homes, three_homes, leasing_fee, renewal_fee)
	SELECT 'Athens', one_home, two_homes, three_homes, leasing_fee, renewal_fee FROM new_prices
	WHERE market = 'Atlanta';
INSERT INTO new_prices (
	market, one_home, two_homes, three_homes, leasing_fee, renewal_fee)
	SELECT 'Burlington', one_home, two_homes, three_homes, leasing_fee, renewal_fee FROM new_prices
	WHERE market = 'Greensboro';
INSERT INTO new_prices (
	market, one_home, two_homes, three_homes, leasing_fee, renewal_fee)
	SELECT 'Granbury', one_home, two_homes, three_homes, leasing_fee, renewal_fee FROM new_prices
	WHERE market = 'Dallas-Fort Worth';
INSERT INTO new_prices (
	market, one_home, two_homes, three_homes, leasing_fee, renewal_fee)
	SELECT 'Carson City', one_home, two_homes, three_homes, leasing_fee, renewal_fee FROM new_prices
	WHERE market = 'Reno';
INSERT INTO new_prices (
	market, one_home, two_homes, three_homes, leasing_fee, renewal_fee)
	SELECT 'Bonham', one_home, two_homes, three_homes, leasing_fee, renewal_fee FROM new_prices
	WHERE market = 'Dallas-Fort Worth';
INSERT INTO new_prices (
	market, one_home, two_homes, three_homes, leasing_fee, renewal_fee)
	SELECT 'Gainesville', one_home, two_homes, three_homes, leasing_fee, renewal_fee FROM new_prices
	WHERE market = 'Orlando';
INSERT INTO new_prices (
	market, one_home, two_homes, three_homes, leasing_fee, renewal_fee)
	SELECT 'Longview', one_home, two_homes, three_homes, leasing_fee, renewal_fee FROM new_prices
	WHERE market = 'Dallas-Fort Worth';
INSERT INTO new_prices (
	market, one_home, two_homes, three_homes, leasing_fee, renewal_fee)
	SELECT 'Shelby', one_home, two_homes, three_homes, leasing_fee, renewal_fee FROM new_prices
	WHERE market = 'Charlotte';
INSERT INTO new_prices (
	market, one_home, two_homes, three_homes, leasing_fee, renewal_fee)
	SELECT 'Cedartown', one_home, two_homes, three_homes, leasing_fee, renewal_fee FROM new_prices
	WHERE market = 'Atlanta';
INSERT INTO new_prices (
	market, one_home, two_homes, three_homes, leasing_fee, renewal_fee)
	SELECT 'Salem', one_home, two_homes, three_homes, leasing_fee, renewal_fee FROM new_prices
	WHERE market = 'Portland - Vancouver';
INSERT INTO new_prices (
	market, one_home, two_homes, three_homes, leasing_fee, renewal_fee)
	SELECT 'North Port', one_home, two_homes, three_homes, leasing_fee, renewal_fee FROM new_prices
	WHERE market = 'Tampa';
INSERT INTO new_prices (
	market, one_home, two_homes, three_homes, leasing_fee, renewal_fee)
	SELECT 'Lakeland', one_home, two_homes, three_homes, leasing_fee, renewal_fee FROM new_prices
	WHERE market = 'Tampa';
INSERT INTO new_prices (
	market, one_home, two_homes, three_homes, leasing_fee, renewal_fee)
	SELECT 'Macon', one_home, two_homes, three_homes, leasing_fee, renewal_fee FROM new_prices
	WHERE market = 'Atlanta';
INSERT INTO new_prices (
	market, one_home, two_homes, three_homes, leasing_fee, renewal_fee)
	SELECT 'Santa Rosa', one_home, two_homes, three_homes, leasing_fee, renewal_fee FROM new_prices
	WHERE market = 'Santa Rosa-North Bay';
INSERT INTO new_prices (
	market, one_home, two_homes, three_homes, leasing_fee, renewal_fee)
	SELECT 'Winston', one_home, two_homes, three_homes, leasing_fee, renewal_fee FROM new_prices
	WHERE market = 'Greensboro';
INSERT INTO new_prices (
	market, one_home, two_homes, three_homes, leasing_fee, renewal_fee)
	SELECT 'Palm Bay', one_home, two_homes, three_homes, leasing_fee, renewal_fee FROM new_prices
	WHERE market = 'Orlando';
INSERT INTO new_prices (
	market, one_home, two_homes, three_homes, leasing_fee, renewal_fee)
	SELECT 'Portland', one_home, two_homes, three_homes, leasing_fee, renewal_fee FROM new_prices
	WHERE market = 'Portland - Vancouver';
INSERT INTO new_prices (
	market, one_home, two_homes, three_homes, leasing_fee, renewal_fee)
	SELECT 'Colorado Spings', one_home, two_homes, three_homes, leasing_fee, renewal_fee FROM new_prices
	WHERE market = 'Denver';
INSERT INTO new_prices (
	market, one_home, two_homes, three_homes, leasing_fee, renewal_fee)
	SELECT 'Sanford', one_home, two_homes, three_homes, leasing_fee, renewal_fee FROM new_prices
	WHERE market = 'Raleigh-Durham';
INSERT INTO new_prices (
	market, one_home, two_homes, three_homes, leasing_fee, renewal_fee)
	SELECT 'Dallas', one_home, two_homes, three_homes, leasing_fee, renewal_fee FROM new_prices
	WHERE market = 'Dallas-Fort Worth';
INSERT INTO new_prices (
	market, one_home, two_homes, three_homes, leasing_fee, renewal_fee)
	SELECT 'Durham', one_home, two_homes, three_homes, leasing_fee, renewal_fee FROM new_prices
	WHERE market = 'Raleigh-Durham';
INSERT INTO new_prices (
	market, one_home, two_homes, three_homes, leasing_fee, renewal_fee)
	SELECT 'Rome', one_home, two_homes, three_homes, leasing_fee, renewal_fee FROM new_prices
	WHERE market = 'Atlanta';
INSERT INTO new_prices (
	market, one_home, two_homes, three_homes, leasing_fee, renewal_fee)
	SELECT 'Fernley', one_home, two_homes, three_homes, leasing_fee, renewal_fee FROM new_prices
	WHERE market = 'Reno';
INSERT INTO new_prices (
	market, one_home, two_homes, three_homes, leasing_fee, renewal_fee)
	SELECT 'Jefferson', one_home, two_homes, three_homes, leasing_fee, renewal_fee FROM new_prices
	WHERE market = 'Atlanta';
INSERT INTO new_prices (
	market, one_home, two_homes, three_homes, leasing_fee, renewal_fee)
	SELECT 'Killeen', one_home, two_homes, three_homes, leasing_fee, renewal_fee FROM new_prices
	WHERE market = 'San Antonio';
INSERT INTO new_prices (
	market, one_home, two_homes, three_homes, leasing_fee, renewal_fee)
	SELECT 'San Francisco', one_home, two_homes, three_homes, leasing_fee, renewal_fee FROM new_prices
	WHERE market = 'The Bay';
INSERT INTO new_prices (
	market, one_home, two_homes, three_homes, leasing_fee, renewal_fee)
	SELECT 'Santa Cruz', one_home, two_homes, three_homes, leasing_fee, renewal_fee FROM new_prices
	WHERE market = 'The Bay';
	
--Join tables to see if we are all set

SELECT DISTINCT(msa), market FROM old_numbers
LEFT JOIN new_prices
ON msa = market;

INSERT INTO new_prices (
	market, one_home, two_homes, three_homes, leasing_fee, renewal_fee)
	SELECT 'Colorado Spings', one_home, two_homes, three_homes, leasing_fee, renewal_fee FROM new_prices
	WHERE market = 'Denver';
	
SELECT * FROM new_prices;
	
--Mistyped Colorado Springs, so inserted it again
--Fully join tables now and create view

CREATE VIEW all_numbers AS (
SELECT * FROM old_numbers
JOIN new_prices
ON msa = market);

--Calculate all new fees for each property

SELECT property_id, (one_home * 12) as new_pm_fee,
(rent_total * leasing_fee) as new_leasing_fee, 
(normal_units_count * renewal_fee) as new_renewal_fee FROM all_numbers
WHERE owner_units = 1 AND rent_total > 0
	
UNION ALL

SELECT property_id, (two_homes * normal_units_count * 12) as new_pm_fee, 
(rent_total * leasing_fee) as new_leasing_fee, 
(normal_units_count * renewal_fee) as new_renewal_fee FROM all_numbers
WHERE owner_units = 2 AND rent_total > 0
	
UNION ALL
	
SELECT property_id, (three_homes * normal_units_count * 12) as new_pm_fee,
(rent_total * leasing_fee) as new_leasing_fee, 
(normal_units_count * renewal_fee) as new_renewal_fee FROM all_numbers
WHERE owner_units > 2 AND rent_total > 0;

--Cannot create new columns in all_numbers view and update them, so will join above query to our original numbers table
--Create view for our final metrics 

CREATE VIEW final_metrics AS (
SELECT owner_id, owner_onboarded_date, old_numbers.property_id, managed_since, normal_units_count,
msa, region_name, hub_name, rent_total, total_old_pm_fees, old_renewal_fee,
old_leasing_fee, owner_units, new_pm_fee, new_leasing_fee, new_renewal_fee
FROM old_numbers
JOIN(

SELECT property_id, (one_home * 12) as new_pm_fee,
(rent_total * leasing_fee) as new_leasing_fee, 
(normal_units_count * renewal_fee) as new_renewal_fee FROM all_numbers
WHERE owner_units = 1 AND rent_total > 0
	
UNION ALL

SELECT property_id, (two_homes * normal_units_count * 12) as new_pm_fee, 
(rent_total * leasing_fee) as new_leasing_fee, 
(normal_units_count * renewal_fee) as new_renewal_fee FROM all_numbers
WHERE owner_units = 2 AND rent_total > 0
	
UNION ALL
	
SELECT property_id, (three_homes * normal_units_count * 12) as new_pm_fee,
(rent_total * leasing_fee) as new_leasing_fee, 
(normal_units_count * renewal_fee) as new_renewal_fee FROM all_numbers
WHERE owner_units > 2 AND rent_total > 0) as new_fees
ON old_numbers.property_id = new_fees.property_id);

--Check view

SELECT * FROM final_metrics
LIMIT 5;

--Looking at the total viability of this plan and if we implemented it everywhere how much it would improve our ARR
--Assume 80% of tenants renew and 20% don't (since the renewal and leasing fees cannot happen at the same time)

SELECT SUM(new_pm_fee-total_old_pm_fees) + (.8 * SUM(new_renewal_fee-old_renewal_fee)) +
(.2 * SUM(new_leasing_fee-old_leasing_fee))
FROM final_metrics;

--See we would improve total ARR by almost $700k
--Checking each individual metrics to see where that profit is coming from, pm fees or renewal/leasing fees

SELECT SUM(new_pm_fee-total_old_pm_fees)
FROM final_metrics;

SELECT (.8 * SUM(new_renewal_fee-old_renewal_fee))
FROM final_metrics;

SELECT (.2 * SUM(new_leasing_fee-old_leasing_fee))
FROM final_metrics;

--Notice we are losing a lot of money by changing the PM fee structure, gaining a lot with leasing/renewal fees
--Group by msa to see which one adds to our ARR the most by each metric (and in total)

SELECT msa, SUM(new_pm_fee-total_old_pm_fees) as pm, SUM(new_renewal_fee-old_renewal_fee) as renewal,
SUM(new_leasing_fee-old_leasing_fee) as leasing
FROM final_metrics
GROUP BY msa
ORDER BY pm DESC;

SELECT msa, SUM(new_pm_fee-total_old_pm_fees) as pm, SUM(new_renewal_fee-old_renewal_fee) as renewal,
SUM(new_leasing_fee-old_leasing_fee) as leasing
FROM final_metrics
GROUP BY msa
ORDER BY renewal DESC;

SELECT msa, SUM(new_pm_fee-total_old_pm_fees) as pm, SUM(new_renewal_fee-old_renewal_fee) as renewal,
SUM(new_leasing_fee-old_leasing_fee) as leasing
FROM final_metrics
GROUP BY msa
ORDER BY leasing DESC;

SELECT msa, (SUM(new_pm_fee-total_old_pm_fees) + (.8 * SUM(new_renewal_fee-old_renewal_fee)) +
(.2 * SUM(new_leasing_fee-old_leasing_fee))) as arr_inc
FROM final_metrics
GROUP BY msa
ORDER BY arr_inc DESC;

--Now we know what fee structure changes are most profitable, both as a whole and in markets
--Want to check how many properties (and units) are in each msa - people may leave after a price increase,
--so its important to weight our price increase with how many people we are affecting

SELECT msa, COUNT(*) FROM final_metrics
GROUP BY msa
ORDER BY count DESC;

SELECT msa, SUM(normal_units_count) FROM final_metrics
GROUP BY msa
ORDER BY sum DESC;

--Now we have some data points and insights to suggest to the company, check out the Tableau dashboard for more!
	