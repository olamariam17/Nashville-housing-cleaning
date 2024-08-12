
-- create database
create database nashville_housing;
use nashville_housing;

-- view dataset
select*from nashville_housing;

select count(distinct(year(saledate))) 
from nashville_housingg;

-- create a duplicate table
create table nashville_housing2
select *from nashville_housing;

-- removing duplicates
select * from (select*, row_number() over(partition by parcelid, landuse, propertyaddress, 
saledate, saleprice,legalreference, soldasvacant,ownername,acreage) as row_num
from nashville_housing2) nash
where nash.row_num > 1;

with duplicate_cte as 
                     (select*, row_number() over(partition by parcelid, landuse, propertyaddress, saledate,
					 saleprice,legalreference, soldasvacant,ownername,acreage) as row_num
					 from nashville_housing2)
delete from duplicate_cte
where row_num > 1;

-- the above syntax did not work so i have to create another table like nashville_housing2 adding row_num as a column.
-- copied the create statement of nashville_housing2 to clip board and added the row_num column.


CREATE TABLE `nashville_housingg` (
  `UniqueID` text,
  `ParcelID` text,
  `LandUse` text,
  `PropertyAddress` text,
  `SaleDate` text,
  `SalePrice` text,
  `LegalReference` text,
  `SoldAsVacant` text,
  `OwnerName` text,
  `OwnerAddress` text,
  `Acreage` text,
  `TaxDistrict` text,
  `LandValue` text,
  `BuildingValue` text,
  `TotalValue` text,
  `YearBuilt` text,
  `Bedrooms` text,
  `FullBath` text,
  `HalfBath` text,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- insert values from nashville_housing to the new created table
select*from nashville_housingg;
 insert into nashville_housingg
select uniqueid, parcelid,landuse, propertyaddress, saledate, saleprice, legalreference,
 soldasvacant, ownername,owneraddress, acreage, taxdistrict, landvalue, buildingvalue, totalvalue, 
 yearbuilt, bedrooms, fullbath,halfbath, 
	row_number() over(partition by parcelid, landuse, propertyaddress, saledate, saleprice,legalreference,
                           soldasvacant,ownername,acreage) as row_num
from nashville_housing2;

-- deleting the duplicte values 
delete from nashville_housingg where row_num >1;


-- Correcting spelling errors
select*from nashville_housingg;
select distinct landuse from nashville_housingg;

-- updating the error
update nashville_housingg
set landuse = 'VACANT RESIDENTIAL LAND'
where landuse like 'VACANT RES%';

-- standardizing data
select distinct soldasvacant from nashville_housingg;
update nashville_housingg
set soldasvacant = 'Yes'
where soldasvacant = 'Y';

update nashville_housingg
set soldasvacant = 'No'
where soldasvacant = 'N';

-- changing saledata format to date
select* from nashville_housingg;
select saledate, str_to_date(saledate, '%M %d, %Y') as salesdate from nashville_housingg;
update nashville_housingg
set saledate = str_to_date(saledate, '%M %d, %Y');
alter table nashville_housingg
modify saledate date;

-- changing salesprice to int
update nashville_housingg
set saleprice = replace(saleprice, ',','');
update nashville_housingg
set saleprice = replace(saleprice, '$', '');
alter table nashville_housingg
modify saleprice int;

-- changing Acreage to decimal
-- changed the blank value to a null value
select acreage from nashville_housingg where acreage = '';
update nashville_housingg
set acreage = null
where acreage = '';
alter table nashville_housingg
modify acreage decimal(10,2);

-- changing landvalue to int
update nashville_housingg
set landvalue = null
where landvalue = '';
alter table nashville_housingg
modify landvalue int;

-- changing buildingvalue to int
update nashville_housingg
set buildingvalue = null
where buildingvalue = '';
alter table nashville_housingg
modify buildingvalue int;

-- changing total value to int 
update nashville_housingg
set totalvalue = null
where totalvalue = '';
alter table nashville_housingg
modify totalvalue int;

-- changing yearbuilt to int 
update nashville_housingg
set yearbuilt = null
where yearbuilt = '';
alter table nashville_housingg
modify yearbuilt int;

-- changing fullbath to int 
update nashville_housingg
set fullbath = null
where fullbath = '';
alter table nashville_housingg
modify fullbath int;

-- changing bedrooms to int 
update nashville_housingg
set bedrooms = null
where bedrooms = '';
alter table nashville_housingg
modify bedrooms int;

-- changing halfbath to int 
update nashville_housingg
set halfbath = null
where halfbath = '';
alter table nashville_housingg
modify halfbath int;

select*from nashville_housingg;
select *from nashville_housingg where PropertyAddress = '';
select distinct parcelid from nashville_housingg;

-- all property address have unique parcelid; meaning 2 or 3 propertyaddress have the same parcelid
-- so we would use the parcelid to populate the null propertyadress using the propertyaddress from another 
--  row with the same parcelid but a different uniqueid
update nashville_housingg
set propertyaddress = null
where propertyaddress = '';

update nashville_housingg as a
join nashville_housingg as b
     on a.parcelid = b.parcelid
     and a.uniqueid <> b.uniqueid
set a.propertyaddress = ifnull(a.propertyaddress, b.propertyaddress)
where a.propertyaddress is null;  

select*from nashville_housingg where ParcelID = '033 06 0A 002.00';

-- Splitting property address column into address and city
-- adding new address and city column
alter table nashville_housingg
add column property_address varchar(100),
add column property_city varchar(100);

-- updating each columns
update nashville_housingg
set property_address = trim(substring(propertyaddress, 1, locate(',', propertyaddress)-1));

update nashville_housingg
set property_city = trim(substring(propertyaddress, locate(',', propertyaddress)+1));

select propertyaddress, property_address, property_city from nashville_housingg;

-- splitting owneraddress into address, city and state
alter table nashville_housingg
add column owner_address varchar(100),
add column owner_city varchar(100),
add column owner_state varchar(100);

-- updating each columns 
update nashville_housingg
set owner_address = trim(substring(owneraddress, 1, locate(',', owneraddress) -1));
update nashville_housingg
set owner_city = trim(substring(owneraddress,locate(',', owneraddress)+1,locate(',', owneraddress,
                 locate(',',owneraddress)+1) - locate(',', owneraddress) -1));
update nashville_housingg
set owner_state = trim(substring(owneraddress, locate(',', owneraddress, locate(',', owneraddress)+1)+1));

select owneraddress, owner_address, owner_city, owner_state from nashville_housingg;
-- deleting unnecessary column
alter table nashville_housingg
drop row_num;

select*from nashville_housingg where OwnerAddress = '';


-- handling missing values; over 50% (30405) of rows have missing values in the table
-- removing all incomplete records to another table and then deleting them from the table
-- created a duplicate table of the cleaned table before deleting the incomplete rows

create table nashville_housingg2
select*from nashville_housingg; 

create table incomplete_nashville_housing
select*from nashville_housingg
where OwnerName is null
or OwnerAddress is null
or acreage is null
or TaxDistrict is null
or landvalue is null
or buildingvalue is null
or totalvalue is null
or yearbuilt is null
or bedrooms is null
or fullbath is null
Or halfbath is null;

delete from nashville_housingg
where OwnerName is null
or OwnerAddress is null
or acreage is null
or TaxDistrict is null
or landvalue is null
or buildingvalue is null
or totalvalue is null
or yearbuilt is null
or bedrooms is null
or fullbath is null
Or halfbath is null;

select * from nashville_housingg;
select * from nashville_housingg2;
select * from incomplete_nashville_housing;

















