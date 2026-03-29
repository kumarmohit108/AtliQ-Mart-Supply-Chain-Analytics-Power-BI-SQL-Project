CREATE VIEW `vw_product_fill_rate_monthly` as (
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
