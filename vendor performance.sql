create database Inventory;
use Inventory;

create table sales(
InventoryId varchar(50),
Store int,
Brand int,
Description varchar(50),
Size varchar(50),
SalesQuantity int,	
SalesDollars float,
SalesPrice float,
SalesDate date,
Volume int,
Classification int,	
ExciseTax float,
VendorNo int,
VendorName varchar(max)
)


bulk insert sales
from 'C:\Users\Lenovo\Desktop\Vendor performance data\sales.csv'
with (
FIRSTROW = 2,
FIELDTERMINATOR = ',',
ROWTERMINATOR = '\n',
batchsize = 250000
);

select * from begin_inventory;
select * from end_inventory;
select * from purchase_prices;
select * from purchases;
select top 1000 * from sales;
select * from vendor_invoice;

--Exploitory data analysis

select count(*) as begin_inventory  from begin_inventory;
select count(*) as end_inventory from end_inventory;
select count(*) as purchase_prices from purchase_prices;
select count(*) as purchases from purchases;
select count(*) as sales from sales;

--lets pick 1 vendor and check how the data is stored across table
select * from purchases 
where VendorNumber = 4466

select * from purchase_prices
where VendorNumber = 4466

select * from sales
where VendorNo = 4466

--#all the information about purchase_price is present in purchases table datewise 
--#PurchasePrice in purchases table dervied from purchase price table

select brand, purchaseprice, sum(Quantity) as totalQuantity,sum(Dollars) as totalPurchaseDollars
from purchases
group by brand, purchaseprice
order by brand

select Brand,sum(SalesDollars) as Tsaledollar,sum(SalesPrice) as Tsaleprice,sum(SalesQuantity) as Tsalequantity from sales
group by brand
order by brand

--Again POnumber,paydate, etc. in Purchases table is derived from vendor_invoice table


--key points
--#the purchases table contain actual purchase data, including date of purchase, products (brands) purchased by
--vendors, the amount paid (in dollars), and the quantity purchased.

--#the purchase price column is derived from the purchase_price table, which provide products wise actual and
--purchase prices. the combination of brand and vendor is unique in this table.

--#the vendor invoice table aggregates data from the purchases table, summerizing quanitity and dollars amounts
--,along with addition column for freight.this table maintains uniqueness based on POnumber and vendor

--#the sales table capture actual sales transaction, detailing the brand purchased by vendors, the quantity sold
--,the selling price and the revenue earned

--# as the data we need for analysis is distributed in different tables, we need to create a aggregated
--table containing:-
--1.purchase transaction made by vendors
--2.sales transaction data
--3.frieght cost for each vendor
--4.actual products price from vendors


--create summary table
CREATE TABLE vendor_sales_summary (
    VendorNumber INT,
	VendorName NVARCHAR(50),
    Brand INT,
	Description NVARCHAR(50),
	PurchasePrice DECIMAL(18, 2),
    ActualPrice DECIMAL(18, 2),
    Volume NVARCHAR(50),
	TotalPurchaseQantity INT,
	TotalPurchaseDollars DECIMAL(18, 2),
	TotalSalesQuantity DECIMAL(18, 2),
    TotalSalesPrice DECIMAL(18, 2),
    TotalDollars DECIMAL(18, 2),
    TotalExciseTax DECIMAL(18, 2),
    FreightCOst DECIMAL(18, 2),
);

--freight summary

with FreightSummary as (
	select
		VendorNumber,
		sum(freight) as FreightCost
	from vendor_invoice
	group by VendorNumber
) select * into 
#TempFrieghtSummary
from FreightSummary;

select * from #TempFrieghtSummary

--Purchases summary 
SELECT *
INTO #TempPurchaseSummary
FROM ( 
     SELECT 
         p.VendorNumber,
         p.VendorName,
         p.Brand,
         p.Description,
         p.PurchasePrice,
         pp.Price as ActualPrice,
         pp.Volume,
         SUM(p.Quantity) as TotalPurchaseQuantity,
         SUM(p.Dollars) as TotalPurchaseDollars
     FROM purchases p
     LEFT JOIN purchase_prices pp
       ON p.Brand = pp.Brand
     WHERE p.PurchasePrice > 0
     GROUP BY 
         p.VendorNumber,
         p.VendorName,
         p.Brand,
         p.Description,
         p.PurchasePrice,
         pp.Price,
         pp.Volume
) as PurchaseSummary;

select * from #TempPurchaseSummary

--Sales summary

select * into #TempSalesSummary
from
(
select
    VendorNo,
	Brand,
    SUM(SalesDollars) AS totalSalesDollars,
    SUM(SalesPrice) AS totalSalesPrice,
    SUM(SalesQuantity) AS totalSalesQuantity,
    SUM(ExciseTax) AS totalExciseTax
from sales 
group by VendorNo,Brand
) as SalesSummary

--Combining all summary tables 

insert into vendor_sales_summary
select 
ps.VendorNumber,
ps.VendorName,
ps.Brand,
ps.Description,
ps.PurchasePrice,
ps.ActualPrice,
ps.Volume,
ps.TotalPurchaseQuantity,
ps.TotalPurchaseDollars,
ss.totalSalesQuantity,
ss.totalSalesPrice,
ss.totalSalesDollars,
ss.totalExciseTax,
fs.FreightCost
from #TempPurchaseSummary ps
left join #TempSalesSummary ss
on ps.Brand = ss.Brand and ps.VendorNumber = ss.VendorNo
left join #TempFrieghtSummary fs
on ps.VendorNumber = fs.VendorNumber
Order by totalPurchaseDollars DESC;

select* from vendor_sales_summary;
drop table new_vendor_sales_summary;