import pandas as pd
from datetime import datetime
from context import cnx

traffic_query = '''
select 
    t.* 
    from similarweb_traffic_metrics t
    	left join companies c on t."domain" = c.primary_url
    	left join score_v2.company_locations cl on cl.company_id = c.company_id
    where last_3_months_mean > 2500
	and last_3_months_mean_bucket in ('low', 'med','high')
    and cl.spc_geo = 'SEA'
'''

if __name__ == '__main__':
    conn = cnx.Cnx

    # pull data
    print('[{}] Pulling traffic...'.format(datetime.now()))
    raw_traffic = pd.read_sql(traffic_query, conn)
    print('[{}] Done pulling traffic.'.format(datetime.now()))

    # group by last_3_months_mean_bucket, and get the top r-squared for each bucket
    print('[{}] Grouping traffic...'.format(datetime.now()))
    traffic = raw_traffic.copy()
    traffic['last_3_months_mean_bucket'] = traffic['last_3_months_mean_bucket'].astype(
        'category'
    )
    traffic_sorted = traffic.sort_values('r_squared', ascending=False)
    traffic_grouped = traffic_sorted.groupby('last_3_months_mean_bucket')
    traffic['ranks'] = traffic_grouped['r_squared'].rank(method='min', ascending=False)

    # assign flag to top 25 of each
    traffic['is_traffic_priority'] = traffic['ranks'] <= 25

    # write to db
    print('[{}] Writing to db'.format(datetime.now()))
    traffic['generated_at'] = datetime.now()
    traffic.to_sql(
        'traffic_flags', conn, if_exists='replace', index=False, schema='score_v2'
    )
    print('[{}] Done writing to db'.format(datetime.now()))
