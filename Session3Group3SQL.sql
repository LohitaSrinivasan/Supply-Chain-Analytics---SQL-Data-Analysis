.import /depot/tdm-mursix/data/BAIM/Manufacturing Processes/Parts.csv PARTS
.import /depot/tdm-mursix/data/BAIM/Manufacturing Processes/Part_Revision.csv PART_REVISION
.import /depot/tdm-mursix/data/BAIM/Manufacturing Processes/Workcenter.csv WORKCENTER
.import /depot/tdm-mursix/data/BAIM/Manufacturing Processes/Parts_Operations.csv PARTS_OPERATIONS
.import /depot/tdm-mursix/data/BAIM/Manufacturing Processes/Workcenter_Log.csv WORKCENTER_LOG

.import /depot/tdm-mursix/data/BAIM/Manufacturing Processes/SupplierOrders.csv ORDER
.import /depot/tdm-mursix/data/BAIM/Manufacturing Processes/Part_Orders.csv PART_ORDERS
.import /depot/tdm-mursix/data/BAIM/Manufacturing Processes/Operations.csv OPERATIONS
.import /depot/tdm-mursix/data/BAIM/Manufacturing Processes/SupplierInfo.csv SUPPLIER
.import /depot/tdm-mursix/data/BAIM/Manufacturing Processes/Part_SupplierJoined.csv PARTS_SUPPLIER

.import /depot/tdm-mursix/data/BAIM/Manufacturing Processes/CustPO_Parts.csv PO_PARTS
.import /depot/tdm-mursix/data/BAIM/Manufacturing Processes/CustPO.csv PO
.import /depot/tdm-mursix/data/BAIM/Manufacturing Processes/Part_Wkc.csv PART_WORKCENTER
.import /depot/tdm-mursix/data/BAIM/Manufacturing Processes/Customer.csv CUSTOMER
.import /depot/tdm-mursix/data/BAIM/Manufacturing Processes/CustomerShipping.csv SHIP

.import /depot/tdm-mursix/data/BAIM/Manufacturing Processes/ZipCodeCombined.csv ZIPCODE




-- PART Operation Delay Analysis
select 
part_op.Part_Operation_Key, part_op.Part_Key, part_op.Operation_Key, part_op.Description, part_op.Standard_Quantity, part_op.Net_Weight, part_op.Delay_Before,
part_op.Delay_After, part_op.Minimum_Quantity, part.Part_Key, part.Part_No, part.Name, rev.Revision, rev.Revision_Key, part.Part_Type, part.Part_Status,
part.Weight, rev.Revision_Effective_Date, rev.Revision_Expiration_Date, part.Minimum_Inventory_Quantity, part.Maximum_Inventory_Quantity,
part.Standard_Job_Quantity, part.Minimum_Job_Quantity, part.Lead_Time_Days, part.Minimum_Order_Quantity, part.Standard_Order_Quantity, wkc.Workcenter_Key,
wkc.Workcenter_Code, wkc.Workcenter_Type, wkc.Name, wkc.Active, wkc.Department_No, log.Log_Key, log.Workcenter_Status_Key, log.Workcenter_Status_Description,
log.Log_Date, log.Log_Hours, log.Part_Operation_Key, log.Credit_Hours 
from Parts_Operations part_op join
Parts part
on part_op.Part_Key = part.Part_Key
join Workcenter_Log log
on log.Part_Operation_key = part_op.Part_Operation_Key
join Workcenter wkc
on wkc.Workcenter_Key = log.Workcenter_Key
join Part_Revision rev
on rev.Part_Key = part.Part_Key
where part.Part_Status = 'Active'
and part.Part_Key = '3005805'
order by log.Credit_Hours desc


-- Unplanned Downtime caused losses
select 
wkc.Workcenter_Key,
wkc.Workcenter_Code,
wkc.Workcenter_Type,
wkc.Name,
wkc.Department_No,
log.Log_Key,
log.Workcenter_Status_Key,
log.Workcenter_Status_Description,
log.Log_Date,
sum(log.Log_Hours)
from Workcenter wkc
join Workcenter_Log log
on wkc.Workcenter_Key = log.WorkCenter_Key
group by wkc.Workcenter_Key, log.Log_Date
HAVING log.Workcenter_Status_Description = "Unplanned Downtime"


-- Part Manufacturing Delay
select 
part.Part_Key,
part_op.Delay_Before+part_op.Delay_After,
part_op.Operation_Key,
part_op.Part_Operation_Description
from PARTS part
join PARTS_OPERATIONS part_op
on part.Part_Key = part_op.Part_Key

-- Workcenter Idle Delay
select 
Workcenter_Key,
sum(Log_Hours),
count(Log_Key)
from WORKCENTER_LOG
group by Workcenter_Key
HAVING Workcenter_Status_Description='IDLE'

-- Shipping Delay Time in Indiana
select 
ship.Customer_No,
po.PO_Key,
datediff(Delivery_Date,Ship_Date) as Duration,
datediff(Ship_Date, Scheduled_Ship_Date) as PreShipDelay
from SHIP as ship
join PO as po
on po.Customer_No = ship.Customer_No
where Customer_No in (select distinct(Customer_No) from CUSTOMER where ZipCode like '47%' or ZipCode like '46%')


-- Seasonality of delay?
SELECT MONTH(Ship_Date),COUNT(*)
FROM 
SHIP
group by month(Ship_Date)
WHERE DATEDIFF(Ship_Date,Scheduled_Ship_Date)>0


-- Supply Chain Delay because of Supplier

select 
part_sup.Approved_Supplier_History_Key,
sup.Supplier_No,
part_sup.Part_Key,
Temp.Scheduled_Ship_Date,
Temp.Ship_Date
from PARTS_SUPPLIER part_sup, SUPPLIER sup
join 
(select PO_Key,
ship.Delivery_Date,
ship.Ship_Date,
po_part.Part_Key
from PO po
join SHIP ship
on po.Customer_No = ship.Customer_No
join PO_PARTS as po_part
on po.PO_No = po_part.Cust_PO_No) as Temp
on part_sup.Part_Key = Temp.Part_Key
where part_sup.Approved_Supplier_History_Key = sup.Approved_Supplier_Key


-- Count of suppliers for each PART
select count(Approved_Supplier_History_Key),Part_Key
from PARTS_SUPPLIER
group by Part_Key


-- Count of orders for each part
select count(Cust_PO_No), Part_Key
from PO_PARTS
group by Part_Key


-- Count of orders placed by each customer
select count(Cust_PO_No), Customer_No
from PO
group by Customer_No

-- Count of operations for each part
select count(Operation_Key),Part_Key
from PARTS_OPERATIONS
group by Part_Key

-- Inventory caused delay? Analyze whether the inventory quantity is causing PreShipDelay
select 
ship.Customer_No,
po.PO_Key,
po.Part_Key,
part.Inventory_Min,
part.Inventory_Max,
part.Lead_Time_Days,
datediff(ship.Delivery_Date,ship.Ship_Date) as Duration,
datediff(ship.Ship_Date, ship.Scheduled_Ship_Date) as PreShipDelay,
from SHIP as ship
join PO as po
on po.Customer_No = ship.Customer_No
join PO_PARTS as po_part
on po_part.Cust_PO_No = po.Cust_PO_No
inner join PART as part
on po_part.Part_Key = part.Part_Key 
