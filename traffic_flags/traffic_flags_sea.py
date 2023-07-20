import pandas as pd
from datetime import datetime
from context import cnx
from sqlalchemy.orm import sessionmaker


traffic_query = '''
select 
    t.*
    , c.company_id 
    from similarweb_traffic_metrics t
    	left join companies c on t."domain" = c.primary_url
    	left join score_v2.company_locations cl on cl.company_id = c.company_id
    where last_3_months_mean > 2500
	and last_3_months_mean_bucket in ('low', 'med','high')
    and cl.spc_geo = 'SEA'
'''

delete_traffic_query = '''
    DO
    $$
    BEGIN
        IF EXISTS (
            SELECT FROM information_schema.tables 
            WHERE  table_schema = 'score_v2'
            AND    table_name   = 'traffic_flags'
        )
        THEN 
            DELETE FROM score_v2.traffic_flags WHERE spc_geo = 'SEA';
        END IF;
    END
    $$
'''

if __name__ == '__main__':
    print('[{}] Starting traffic_flags_sea.py'.format(datetime.now()))
    conn = cnx.Cnx
    session = sessionmaker(bind=conn)()

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
    traffic['is_traffic_priority'] = traffic['ranks'] <= 50

    # write to db
    print('[{}] Deleting old SEA data'.format(datetime.now()))
    try:
        session.execute(delete_traffic_query)
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()
    print('[{}] Writing to db'.format(datetime.now()))
    to_write = traffic[['company_id', 'is_traffic_priority']]
    to_write['generated_at'] = datetime.now()
    to_write['spc_geo'] = 'SEA'
    to_write.to_sql(
        'traffic_flags', conn, if_exists='append', index=False, schema='score_v2'
    )
    print('[{}] Done writing to db'.format(datetime.now()))
