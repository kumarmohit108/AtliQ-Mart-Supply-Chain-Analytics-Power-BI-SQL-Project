CREATE VIEW `vw_customer_otif_monthly` as (
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
