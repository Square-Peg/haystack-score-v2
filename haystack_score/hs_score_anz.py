import pandas as pd
from datetime import datetime
from context import cnx
import numpy as np
from sqlalchemy.orm import sessionmaker

SPC_GEO = 'ANZ'

hs_company_query = '''
    select 
        r.company_id
        , rf.is_irrelevant_role
        , cf.is_irrelevant as is_irrelevant_company
    from roles r
    left join score_v2.company_locations cl on cl.company_id = r.company_id
    left join score_v2.role_flags rf on rf.role_id = r.role_id
    left join score_v2.person_flags pf on pf.person_id = r.person_id
    left join score_v2.company_flags cf on cf.company_id = r.company_id
    where 
        rf.is_founder = TRUE
        and pf.currently_undergrad = FALSE
        and r.role_start > '2019-01-01'
        and r.role_end is null
        and cl.spc_geo = '{}'
'''.format(
    SPC_GEO
)

sweetspot_query = '''
    select
        *
    from score_v2.company_sweetspot_flags
    where company_id in (
    select distinct company_id from score_v2.company_locations where spc_geo = '{}'
    )
'''.format(
    SPC_GEO
)

traffic_flags_query = '''
    SELECT
        *
    from score_v2.traffic_flags
    where company_id in (select distinct company_id from score_v2.company_locations where spc_geo = '{}')
'''.format(
    SPC_GEO
)

person_score_query = '''
    select
        ps.*
        , r.company_id
    from score_v2.person_scores ps
    left join roles r on r.person_id = ps.person_id
    where r.company_id in (select distinct company_id from score_v2.company_locations where spc_geo = '{}')
'''.format(
    SPC_GEO
)

delete_haystack_score_query = '''
    DO
    $$
    BEGIN
        IF EXISTS (
            SELECT FROM information_schema.tables 
            WHERE  table_schema = 'score_v2'
            AND    table_name   = 'haystack_scores'
        )
        THEN 
            DELETE FROM score_v2.haystack_scores WHERE spc_geo = '{}';
        END IF;
    END
    $$
'''.format(
    SPC_GEO
)

if __name__ == '__main__':
    print('[{}] Starting hs_score_{}.py...'.format(datetime.now(), SPC_GEO.lower()))
    conn = cnx.Cnx

    # get company list
    print('[{}] Getting company list...'.format(datetime.now()))
    raw_hs_company = pd.read_sql_query(hs_company_query, conn)
    company_list = raw_hs_company['company_id'].tolist()
    deduped_company_list = list(set(company_list))
    company_list_string = ','.join([str(x) for x in deduped_company_list])
    print(
        '[{}] Company list length: {}'.format(datetime.now(), len(deduped_company_list))
    )

    # get flags and intermediate scores
    print('[{}] Getting flags and intermediate scores...'.format(datetime.now()))
    sweetspot_flags = pd.read_sql_query(sweetspot_query, conn)
    traffic_flags = pd.read_sql_query(traffic_flags_query, conn)
    person_scores = pd.read_sql_query(person_score_query, conn)
    print('[{}] Fetched flags and intermediate scores'.format(datetime.now()))

    # calculate mean founder scores
    print('[{}] Calculating mean founder scores...'.format(datetime.now()))
    person_scores_deduped = person_scores.drop_duplicates(subset=['person_id'])
    company_person_scores_mean = (
        person_scores.groupby(['company_id']).agg({'score': 'mean'}).reset_index()
    )
    company_person_scores_mean.columns = ['company_id', 'founder_score_mean']
    print('[{}] Calculated mean founder scores'.format(datetime.now()))

    # aggregate by company
    print('[{}] Aggregating by company...'.format(datetime.now()))
    company_df = (
        raw_hs_company.groupby(['company_id'])
        .agg({'is_irrelevant_role': 'any', 'is_irrelevant_company': 'any'})
        .reset_index()
    )

    company_df = (
        company_df.merge(company_person_scores_mean, how='left', on='company_id')
        .merge(sweetspot_flags, how='left', on='company_id')
        .merge(traffic_flags, how='left', on='company_id')
    )
    print('[{}] Aggregated by company'.format(datetime.now()))

    # calculate company score
    print('[{}] Calculating Haystack score...'.format(datetime.now()))
    company_df['has_nonzero_founder_score'] = np.where(
        company_df['founder_score_mean'] > 0, True, False
    )
    company_df['sweetspot_value'] = np.where(
        company_df['is_sweetspot_company'] == True, 2, 0
    )
    company_df['traffic_value'] = np.where(
        company_df['is_traffic_priority'] == True, 5, 0
    )
    company_df['founder_score_mean'] = company_df['founder_score_mean'].fillna(0)
    company_df['hs_score_v2'] = (
        company_df['founder_score_mean']
        + company_df['sweetspot_value']
        + company_df['traffic_value']
    )

    # check if is_irrelevant
    company_df['is_irrelevant_hs'] = (
        company_df['is_irrelevant_role'] | company_df['is_irrelevant_company']
    )
    company_df['is_irrelevant_hs'] = company_df['is_irrelevant_hs'].fillna(False)

    # set hs_score_v2 to negative if is_irrelevant_hs
    company_df['hs_score_v2'] = np.where(
        company_df['is_irrelevant_hs'] == True,
        company_df['hs_score_v2'] * -1,
        company_df['hs_score_v2'],
    )
    print('[{}] Calculated Haystack score'.format(datetime.now()))

    # create metadata columns
    print('[{}] Creating metadata columns...'.format(datetime.now()))

    # write to db
    print('[{}] Deleting old {} haystack scores...'.format(datetime.now(), SPC_GEO))
    try:
        session = sessionmaker(bind=conn)()
        session.execute(delete_haystack_score_query)
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()
    print('[{}] Writing to db...'.format(datetime.now()))
    to_write = company_df[
        [
            'company_id',
            'hs_score_v2',
            'is_sweetspot_company',
            'is_traffic_priority',
            'is_irrelevant_hs',
            'founder_score_mean',
        ]
    ]
    to_write['generated_at'] = datetime.now()
    to_write['spc_geo'] = SPC_GEO
    to_write.to_sql(
        'haystack_scores', conn, if_exists='append', index=False, schema='score_v2'
    )
    print('[{}] Wrote to db'.format(datetime.now()))
