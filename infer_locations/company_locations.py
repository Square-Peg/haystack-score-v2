import pandas as pd
from context import cnx
from datetime import datetime

person_locations_query = '''
    select
        r.company_id
        , l.spc_geo
        , l.inferred_country_code_alpha3
        , l.inferred_city
        , r.last_scraped_at
    from roles r
        left join score_v2.person_locations l on l.person_id = r.person_id
        left join score_v2.role_flags rf on rf.role_id = r.role_id
    where l.spc_geo is not null
        and rf.is_founder = true
        and r.role_end is null
        and r.company_id is not null
        '''

if __name__ == '__main__':
    # pull data
    print('[{}] Starting...'.format(datetime.now()))
    conn = cnx.Cnx
    raw_person_locations = pd.read_sql_query(person_locations_query, conn)
    print(
        '[{}] Done pulling data. Pulled: {}'.format(
            datetime.now(), len(raw_person_locations)
        )
    )

    person_locations = raw_person_locations.copy()
    person_locations_sorted = person_locations.sort_values(
        'last_scraped_at', ascending=False
    )
    company_locations = person_locations_sorted.drop_duplicates(
        'company_id', keep='first'
    )

    # write to db
    print('[{}] Writing to db'.format(datetime.now()))
    company_locations.to_sql(
        'company_locations', conn, if_exists='replace', index=False, schema='score_v2'
    )
    print('[{}] Done writing to db'.format(datetime.now()))
