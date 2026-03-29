CREATE VIEW `vw_executive_summary_monthly` AS (
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
