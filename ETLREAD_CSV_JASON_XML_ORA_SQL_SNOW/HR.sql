drop table HR.INVENTORY_LOT 

CREATE TABLE HR.INVENTORY_LOT 
(
partnumber VARCHAR2(50),
locationidentifier VARCHAR2(50),
inventorytype VARCHAR2(50),
quantity NUMBER,
UNIT VARCHAR2(10),

EXPDATE DATE,
PARENTTYPE VARCHAR2(50),
CLASSES VARCHAR(50),
segment VARCHAR2(50),
lotCode NUMBER,

status VARCHAR2(10),
value NUMBER ,
currency VARCHAR2(30) ,
source VARCHAR2(30),
storedate DATE 
)

PS-SL-A287,
LT-1,
PRODUCT,
25,
EA,

2022-12-31T00:00:00,
ONHAND,
NEW,
INDUSTRIAL,
521033,

Active,
31250,
USD,
https://foo.com,
2022-01-01T00:00:00


SELECT * FROM hr.INVENTORY_LOT

drop table hr.INVENTORY_LOT 

CREATE TABLE HR.INVENTORY_LOT 
(
partnumber VARCHAR2(50),
location VARCHAR2(50),
type VARCHAR2(50),
quantity NUMBER,
unit VARCHAR2(10),

expdate DATE,
parenttype VARCHAR2(50),
class VARCHAR(50),
segment VARCHAR2(50),
lotCode NUMBER,

status VARCHAR2(10),
value NUMBER ,
currency VARCHAR2(30) ,
link VARCHAR2(30),
storagedate DATE 
)

partnumber,location,type,quantity,unit,
expdate,parentyype,class,segment,lotcode,
status,value,currency,source, storedate

