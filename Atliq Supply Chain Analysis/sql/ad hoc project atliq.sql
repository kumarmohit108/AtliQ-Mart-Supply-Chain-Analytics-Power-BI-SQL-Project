-- create database as atliq_supply_chain
CREATE DATABASE atliq_supply_chain;
USE atliq_supply_chain;

-- create table dim_customers
create table dim_customers (
	customer_id varchar(20),
    customer_name varchar(100),
    city varchar(55)
);

-- create table dim_products

CREATE TABLE dim_products (
    product_id VARCHAR(20),
    product_name VARCHAR(100),
    category VARCHAR(50)
);

-- create table dim_date

CREATE TABLE dim_date (
    date VARCHAR(20),
    mmm_yy VARCHAR(20),
    week_no VARCHAR(10)
);

-- create table dim_targets_orders

CREATE TABLE dim_targets_orders (
    customer_id VARCHAR(20),
    ontime_target VARCHAR(10),
    infull_target VARCHAR(10),
    otif_target VARCHAR(10)
);

-- create table fact_order_lines

CREATE TABLE fact_order_lines (
    order_id VARCHAR(20),
    order_placement_date VARCHAR(55),
    customer_id VARCHAR(20),
    product_id VARCHAR(20),
    order_qty VARCHAR(20),
    agreed_delivery_date VARCHAR(55),
    actual_delivery_date VARCHAR(55),
    delivery_qty VARCHAR(20),
    In_Full tinyint,
	On_Time tinyint,
	On_Time_In_Full tinyint
);

-- CREATE TABLE fact_orders_aggregate
CREATE TABLE fact_orders_aggregate (
    order_id VARCHAR(50),
    customer_id VARCHAR(50),
    order_placement_date VARCHAR(50),
    on_time VARCHAR(5),
    in_full VARCHAR(5),
    otif VARCHAR(5)
);

-- create table staging table/clone table for dim_date
create table dim_date_clean (
	date DATE,
    mmm_yy VARCHAR(25),
    week_no int
);
-- change the date data type using str_to_date(), 
-- remove "W" from "W14" week_no column make it numeric column

INSERT INTO dim_date_clean 
select
	str_to_date(DATE, '%d-%b-%Y'),
    mmm_yy,
    CAST(replace(week_no, 'W', ' ') AS unsigned)
from dim_date;

-- CREATE clone/staging TABLE fact_order_lines_clean

CREATE TABLE fact_order_lines_clean (
    order_id VARCHAR(20),
    order_placement_date DATE,
    customer_id INT,
    product_id INT,
    order_qty INT,
    agreed_delivery_date DATE,
    actual_delivery_date DATE,
    delivery_qty INT,
    in_full TINYINT,
    on_time TINYINT,
    on_time_in_full TINYINT
);

-- insert data with perfect conversion
INSERT INTO fact_order_lines_clean
SELECT
    order_id,
    STR_TO_DATE(order_placement_date, '%W, %M %e, %Y'),
    CAST(customer_id AS UNSIGNED),
    CAST(product_id AS UNSIGNED),
    CAST(order_qty AS UNSIGNED),
    STR_TO_DATE(agreed_delivery_date, '%W, %M %e, %Y'),
    STR_TO_DATE(actual_delivery_date, '%W, %M %e, %Y'),
    CAST(delivery_qty AS UNSIGNED),
    CAST(in_full AS UNSIGNED),
    CAST(on_time AS UNSIGNED),
    CAST(on_time_in_full AS UNSIGNED)
FROM fact_order_lines;
 
-- create cloen/staging table dim_customer_clean 
create table dim_customers_clean (
	customer_id int,
    customer_name varchar(100),
    city varchar(70)
);

-- insert data with perfect conversion
insert into dim_customers_clean
select 
	cast(customer_id as unsigned),
    customer_name,
    city
from dim_customers
;
-- create clone table dim_products_clean
create table dim_products_clean (
	product_id int,
    products_name varchar(100),
    category varchar(50)
);

-- insert data with perfect conversion
insert into dim_products_clean
select
	cast(product_id as unsigned),
    product_name,
	category
from dim_products;

-- create clone table dim_targets_orders_clean
create table dim_targets_orders_clean (
	customer_id int,
    ontime_target decimal(5,2),
    infull_target decimal(5,2),
    otif_target decimal(5,2)
);

-- insert data with perfect conversions
insert into dim_targets_orders_clean 
	select
    cast(customer_id as unsigned),
    cast(ontime_target as decimal(5,2)),
    cast(infull_target as decimal(5,2)),
    cast(otif_target as decimal(5,2))
from dim_targets_orders;
-- create clone/staging table fact_orders_aggregate_clean
 
create table fact_orders_aggregate_clean (
	order_id varchar(50),
    customer_id int,
    order_placement_date date,
    on_time tinyint,
    in_full tinyint,
    otif tinyint
);

-- insert data with perfect conversion in fact_orders_aggregate_clean

INSERT INTO fact_orders_aggregate_clean
(
    order_id,
    customer_id,
    order_placement_date,
    on_time,
    in_full,
    otif
)
SELECT
    order_id,
    CAST(customer_id AS UNSIGNED),
    STR_TO_DATE(order_placement_date, '%d-%b-%Y'),
    CAST(on_time AS UNSIGNED),
    CAST(in_full AS UNSIGNED),
    CAST(otif AS UNSIGNED)
FROM fact_orders_aggregate;

-- actual queries start from here--
-- i made otif truth called order_otif using CTE

with order_otif as(
	select
		order_id,
        customer_id,
        order_placement_date,
        otif
	from fact_orders_aggregate_clean
),
	order_otif_customer as (
    select
		o.order_id,
        o.customer_id,
        c.customer_name,
        c.city,
        o.order_placement_date,
        o.otif
	from order_otif o
    join dim_customers_clean c
    on o.customer_id = c.customer_id
)
select * from order_otif_customer
limit 20;

-- Now join dim_date_clean with previous query 
-- join using previous CTE order_otif_customer

with order_otif as (
	select
		order_id,
        customer_id,
        order_placement_date,
        otif
	from fact_orders_aggregate_clean
),
	order_otif_customer as (
		select
			o.order_id,
            o.customer_id,
            c.customer_name,
            c.city,
            o.order_placement_date,
            o.otif
		from order_otif o
        join dim_customers_clean c
			on o.customer_id = c.customer_id
),
	order_otif_customer_date as(
		select 
			oc.order_id,
            oc.customer_id,
            oc.customer_name,
            oc.city,
            d.mmm_yy as month,
            oc.otif
		from order_otif_customer oc
		join dim_date_clean d
			ON oc.order_placement_date = d.date
)
select 
	*
from order_otif_customer_date
limit 20;

-- otif % by city,month
-- otif% --> group by --> city and month 

with order_otif as (
	select
		order_id,
        customer_id,
        order_placement_date,
        otif
	from fact_orders_aggregate_clean
),
	order_otif_customer as (
		select
			o.order_id,
            o.customer_id,
            c.city,
            o.order_placement_date,
            o.otif
		from order_otif o
        join dim_customers_clean c
			on o.customer_id = c.customer_id
),
	order_otif_customer_date as(
		select 
			oc.order_id,
            oc.customer_id,
            oc.city,
            d.mmm_yy as month,
            oc.otif
		from order_otif_customer oc
		join dim_date_clean d
			ON oc.order_placement_date = d.date
)
select 
	city,
    month,
    round(100 * sum(otif) / count(distinct order_id), 2) as otif_pct
from order_otif_customer_date
group by 
	city,
    month
order by
	city,
    month;
    
-- now i create custome-wise otif %
-- >>write CTEs>>start with order_otif>>join dim_customer with order_otif>> and then join dim_date 
-- group by>>customer_name and month>>

with order_otif as (
	select
		order_id,
        customer_id,
        order_placement_date,
        otif
	from fact_orders_aggregate_clean
),
	order_otif_customer as (
		select
			o.order_id,
            o.customer_id,
            c.customer_name,
            o.order_placement_date,
            o.otif
		from order_otif o
        join dim_customers_clean c
			on o.customer_id = c.customer_id
),
	order_otif_customer_date as (
		select
			oc.order_id,
            oc.customer_name,
            d.mmm_yy as month,
            oc.otif
		from order_otif_customer oc
        join dim_date_clean d
			on oc.order_placement_date = d.date
)
	select
		customer_name,
        month,
        round(100 * sum(otif) / count(distinct order_id), 2) as otif_pct
        from order_otif_customer_date
			group by 
            customer_name,
            month
            order by
            customer_name,
            month;
            
-- now i write CTEs to view product_wise otif_pct 
-- >> using joins >> order otif >> order_otif from fact_orders_aggregate_clean + product lens form dim_product >> order_otif + product lens form dim_products_clean + date from dim_date_clean  

with order_otif as (
	select
		order_id,
        order_placement_date,
        otif
	from fact_orders_aggregate_clean
),
	order_product_map as (
		select distinct 
			o.order_id,
            l.product_id,
            o.order_placement_date,
            o.otif
		from order_otif o
        join fact_order_lines_clean l
			on o.order_id = l.order_id
),
	order_product_date as (
		select	
			op.order_id,
            p.products_name,
            d.mmm_yy as month,
            op.otif
		from order_product_map op
        join dim_products_clean p 
			on op.product_id = p.product_id
		join dim_date_clean d
			on op.order_placement_date =d.date
)
	select
		products_name,
        month,
        round(100 * sum(otif) / count(distinct order_id), 2) as otif_pct
	from order_product_date
    group by
		products_name,
        month
	order by
		products_name,
        month
	;
    
-- actual_otif_pct vs targets_otif_pct and otif_gap
-- join dim_targets_orders_clean table and fetch data of otif_target using CTEs 

with order_otif as (
	select 
		order_id,
        customer_id,
        order_placement_date,
        otif
	from fact_orders_aggregate_clean
),
	order_otif_customer_date as (
		select
			o.order_id,
            o.customer_id,
            c.customer_name,
            d.mmm_yy as month,
            o.otif
		from order_otif o
        join dim_customers_clean c 
			on o.customer_id = c.customer_id
		join dim_date_clean d 
			on o.order_placement_date = d.date
),
	customer_month_otif as (
		select
			customer_id,
            customer_name,
            month,
            round(100 * sum(otif) / count(distinct order_id),2) as actual_otif_pct
		from order_otif_customer_date
        group by
			customer_id,
            customer_name,
            month
)
	select
		cmo.customer_name,
        cmo.month,
        cmo.actual_otif_pct,
        t.otif_target as target_otif_pct,
        round(cmo.actual_otif_pct-t.otif_target, 2) as otif_gap
	from customer_month_otif cmo
    join dim_targets_orders_clean t
		on cmo.customer_id = t.customer_id
	order by
		otif_gap asc;
        
-- LIFR(LINE FILL RATE) AND VOFR(VOLUME FILL RATE)

WITH LINE_STATUS as (
			select
				case
					when delivery_qty >= order_qty then 1
                    else 0
				end as line_fulfilled
			from fact_order_lines_clean
)
	select
		round(100 * sum(line_fulfilled) / count(*),2) as line_fill_rate_pct
	from LINE_STATUS;
        
-- write select query for VOFR %
select
	round(100 * sum(delivery_qty) / sum(order_qty),2) as volume_fill_rate_pct
    from fact_order_lines_clean
    ;
    
-- now add context in LIFR% and VOFR% matrics
with line_status as (
	select
		l.order_id,
		l.product_id,
        l.order_qty,
        l.delivery_qty,
        case
			when l.delivery_qty >= l.order_qty then 1
            else 0
		end as line_fulfilled,
        l.order_placement_date
	from fact_order_lines_clean l 
),
	line_product_month as (
		select
			d.mmm_yy as month,
			p.products_name,
            ls.line_fulfilled,
            ls.order_qty,
			ls.delivery_qty
		from line_status ls
        join dim_products_clean p 
			on ls.product_id = p.product_id
		join dim_date_clean d 
			on ls.order_placement_date = d.date
)
	select
		products_name,
        month,
        round(100 * sum(line_fulfilled) / count(*), 2) as line_fill_rate_pct,
        round(100 * sum(delivery_qty) / sum(order_qty), 2) as volume_fill_rate_pct
	from line_product_month
    group by
		products_name,
        month
	order by
		products_name,
        month;
	
-- executive OTIF + LIFR + VOFR - ONE DECISION _READY VIEW
WITH otif_month as (
	select
		d.mmm_yy as month,
        round(100*sum(o.otif)/count(distinct o.order_id),2) as otif_pct
	from fact_orders_aggregate_clean o 
	join dim_date_clean d
		on o.order_placement_date = d.date
	group by
		month
),
	lfr_vfr_month as (
		select
			 d.mmm_yy as month,
             round(100*sum(
					case 
						when l.delivery_qty >= l.order_qty then 1
                        else 0
					end)/count(*),2) as line_fil_rate_pct,
			round(100 * sum(l.delivery_qty)/sum(l.order_qty),2) as volume_fill_rate_pct
		from fact_order_lines_clean l 
        join dim_date_clean d 
			on l.order_placement_date = d.date
		group by
			month
)        
	select
		o.month,
        o.otif_pct,
        l.line_fil_rate_pct,
        l.volume_fill_rate_pct
	from otif_month o 
    join lfr_vfr_month l 
		on o.month = l.month
	order by
		o.month;

drop view vw_executive_summary_monthly





