import pandas as pd
import numpy as np
import sys

from datetime import datetime
from context import cnx
from sqlalchemy.orm import sessionmaker

SPC_GEO = 'SEA'
CURRENT_DATE = datetime.now().strftime('%Y%m%d')
CURRENT_DATE_WITH_DASH = datetime.now().strftime('%Y-%m-%d')
JUNK_COMPANY_ID_FILEPATH = (
    '/Users/kai/repositories/spc/haystack/haystack-score-v2/data/junk_company_ids.csv'
)

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
        and r.role_start > '2020-01-01'
        and r.role_end is null
        and cl.spc_geo = '{}'
'''.format(
    SPC_GEO
)

sweetspot_query = '''
    select
        company_id
        , is_sweetspot_company
    from score_v2.company_sweetspot_flags sf
    where company_id in (
    select distinct company_id from score_v2.company_locations where spc_geo = '{}'
    )
    and spc_geo = '{}'
'''.format(
    SPC_GEO, SPC_GEO
)

traffic_flags_query = '''
    SELECT
        company_id
        , is_traffic_priority
    from score_v2.traffic_flags
    where company_id in (select distinct company_id from score_v2.company_locations where spc_geo = '{}')
        and spc_geo = '{}'
'''.format(
    SPC_GEO, SPC_GEO
)

person_score_query = '''
    select
        ps.*
        , r.company_id
        , p.linkedin_url
        , p.last_scraped_at
        , p.full_name
    from score_v2.person_scores ps
    left join roles r on r.person_id = ps.person_id
    left join persons p on p.person_id = ps.person_id
    left join score_v2.role_flags rf on rf.role_id = r.role_id
    where r.company_id in (select distinct company_id from score_v2.company_locations where spc_geo = '{}')
    and rf.is_founder = TRUE
'''.format(
    SPC_GEO
)

delete_haystack_score_query_prod = '''
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

delete_haystack_score_query_test = '''
    DO
    $$
    BEGIN
        IF EXISTS (
            SELECT FROM information_schema.tables 
            WHERE  table_schema = 'score_v2'
            AND    table_name   = 'haystack_scores_test'
        )
        THEN 
            DELETE FROM score_v2.haystack_scores_test WHERE spc_geo = '{}';
        END IF;
    END
    $$
'''.format(
    SPC_GEO
)

company_metadata_query = '''
    select
        company_id
        , primary_url as company_primary_url
        , "name" as company_name
    from companies 
'''


def create_note_string(row):
    note = '''Founder Summaries: {founder_summaries}

Founder LinkedIn URLs: {linkedin_urls}

------

Haystack Score: {hs_score:.2f}
Haystack Breakdown:
    Mean Founder Score: {founder_score:.2f}
    Is Sweetspot Company: {sweetspot_company}
    Is Traffic Priority: {traffic_priority}

Haystack Company ID: {haystack_id}
Date Generated: {date_generated}
'''

    founder_summaries = ''
    full_name_list = row['full_name'] if row['full_name'] is not np.nan else [None]
    summary_list = row['description'] if row['description'] is not np.nan else [None]
    linkedin_url_list = (
        row['linkedin_url'] if row['linkedin_url'] is not np.nan else [None]
    )
    for full_name, summary in zip(full_name_list, summary_list):
        if full_name:
            founder_summaries += '\n  {}'.format(full_name)
        if summary:
            founder_summaries += ': {} '.format(summary)
    linkedin_urls = ''
    for li_url in linkedin_url_list:
        if li_url:
            linkedin_urls += '\n  {}'.format(li_url)
    traffic_prio_string = (
        str(row['is_traffic_priority'])
        if pd.notna(row['is_traffic_priority'])
        else 'No data'
    )

    note = note.format(
        founder_summaries=founder_summaries,
        linkedin_urls=linkedin_urls,
        hs_score=row['hs_score_v2'],
        founder_score=row['founder_score_mean'],
        sweetspot_company=str(row['is_sweetspot_company']),
        traffic_priority=traffic_prio_string,
        haystack_id=str(row['company_id']),
        date_generated=CURRENT_DATE_WITH_DASH,
    )

    return note


if __name__ == '__main__':
    print('[{}] Starting hs_score_{}.py...'.format(datetime.now(), SPC_GEO.lower()))
    conn = cnx.Cnx
    if len(sys.argv) < 2:
        print('Provide experiment name.')
        sys.exit(1)

    experiment_name = sys.argv[1]

    if experiment_name == 'prod':
        print('**NOTICE** Running usual prod script.')

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
    person_scores_deduped['linkedin_url'] = person_scores_deduped[
        'linkedin_url'
    ].str.rstrip('/')
    person_scores_deduped = person_scores_deduped.sort_values(
        'last_scraped_at', ascending=False
    ).drop_duplicates('linkedin_url')
    person_scores_deduped = person_scores_deduped.drop_duplicates(
        ['full_name', 'company_id']
    )
    company_person_scores_mean = (
        person_scores_deduped.groupby(['company_id'])
        .agg({'score': 'mean'})
        .reset_index()
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

    print('[{}] Creating notes...'.format(datetime.now()))
    person_scores_deduped['description'] = person_scores_deduped['description'].replace(
        np.nan, None
    )

    all_person_metadata = (
        person_scores_deduped.groupby(
            [
                'company_id',
            ],
            dropna=False,
        )
        .agg(
            {
                'linkedin_url': lambda x: list(x),
                'description': lambda x: list(x),
                'full_name': lambda x: list(x),
            }
        )
        .reset_index()
    )
    company_with_metadata = company_df.merge(
        all_person_metadata, how='left', on='company_id'
    )
    company_with_metadata['notes'] = company_with_metadata.apply(
        create_note_string, axis=1
    )

    company_with_metadata = company_with_metadata.dropna(subset='hs_score_v2')

    # exclude junk IDs
    print(
        '[{}] Excluding junk companies. Before rows: {}...'.format(
            datetime.now(), len(company_with_metadata)
        )
    )
    junk_id_df = pd.read_csv(JUNK_COMPANY_ID_FILEPATH)
    junk_ids = list(junk_id_df['company_id'])
    company_with_metadata = company_with_metadata[
        ~company_with_metadata['company_id'].isin(junk_ids)
    ]
    print(
        '[{}] Done excluding junk companies. Rows: {}'.format(
            datetime.now(), len(company_with_metadata)
        )
    )

    # write to db

    if experiment_name == 'prod':
        # write to main table
        print('[{}] Deleting old {} haystack scores...'.format(datetime.now(), SPC_GEO))
        try:
            session = sessionmaker(bind=conn)()
            session.execute(delete_haystack_score_query_prod)
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()
        print('[{}] Writing to db...'.format(datetime.now()))
        to_write = company_with_metadata[
            [
                'company_id',
                'hs_score_v2',
                'is_sweetspot_company',
                'is_traffic_priority',
                'is_irrelevant_hs',
                'founder_score_mean',
                'notes',
            ]
        ]
        to_write['generated_at'] = datetime.now()
        to_write['spc_geo'] = SPC_GEO
        to_write.to_sql(
            'haystack_scores', conn, if_exists='append', index=False, schema='score_v2'
        )
        print('[{}] Wrote to db'.format(datetime.now()))

    elif experiment_name == 'test':
        # write to test table
        print(
            '[{}] Deleting old {} test haystack scores...'.format(
                datetime.now(), SPC_GEO
            )
        )
        try:
            session = sessionmaker(bind=conn)()
            session.execute(delete_haystack_score_query_test)
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()
        print('[{}] Writing to db...'.format(datetime.now()))
        to_write = company_with_metadata[
            [
                'company_id',
                'hs_score_v2',
                'is_sweetspot_company',
                'is_traffic_priority',
                'is_irrelevant_hs',
                'founder_score_mean',
                'notes',
            ]
        ]
        to_write['generated_at'] = datetime.now()
        to_write['spc_geo'] = SPC_GEO
        to_write.to_sql(
            'haystack_scores_test',
            conn,
            if_exists='append',
            index=False,
            schema='score_v2',
        )
        print('[{}] Wrote to db'.format(datetime.now()))

    else:
        # write to experiment table
        print('[{}] Writing to db...'.format(datetime.now()))
        to_write = company_with_metadata[
            [
                'company_id',
                'hs_score_v2',
                'is_sweetspot_company',
                'is_traffic_priority',
                'is_irrelevant_hs',
                'founder_score_mean',
                'notes',
            ]
        ]
        to_write['generated_at'] = datetime.now()
        to_write['spc_geo'] = SPC_GEO
        to_write['experiment_name'] = experiment_name
        to_write.to_sql(
            'haystack_scores_experiment',
            conn,
            if_exists='append',
            index=False,
            schema='score_v2',
        )
        print('[{}] Wrote to db'.format(datetime.now()))
