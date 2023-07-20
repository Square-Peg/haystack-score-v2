import pandas as pd
from datetime import datetime
from context import cnx
import numpy as np

SPC_GEO = 'ISR'
CURRENT_DATE = datetime.now().strftime('%Y%m%d')
CURRENT_DATE_WITH_DASH = datetime.now().strftime('%Y-%m-%d')
JUNK_COMPANY_ID_FILEPATH = (
    '/Users/kai/repositories/spc/haystack/haystack-score-v2/data/junk_company_ids.csv'
)
_UPLOADS_DIR = '/Users/kai/repositories/spc/haystack/haystack-score-v2/_uploads'


hs_weekly_query = '''
    select hs.*
    , c.primary_url
    , p.linkedin_url
    , p.last_scraped_at
    , c."name" as company_name
    , r.role_id
    , p.person_id
    , p.full_name
    from 
    score_v2.haystack_scores hs
    left join companies c on c.company_id = hs.company_id
    left join roles r on r.company_id = hs.company_id
    left join score_v2.role_flags rf on rf.role_id = r.role_id
    left join persons p on p.person_id = r.person_id
    where hs.hs_score_v2 is not null
    and rf.is_founder is true
    and p.linkedin_url is not null
    and r.role_end is null
    and c.last_scraped_at > '2023-07-01'
    and hs.spc_geo = '{}'
'''.format(
    SPC_GEO
)

person_score_description = '''
    select 
        ps.person_id
        , ps.description
    from score_v2.person_scores ps
    where spc_geo = '{}'
'''.format(
    SPC_GEO
)


def create_note_string(row):
    note = '''
Founder Summaries: {founder_summaries}

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
    for full_name, summary in zip(row['full_name'], row['description']):
        if full_name is not None:
            founder_summaries += '\n  {}'.format(full_name)
        if summary is not None:
            founder_summaries += ': {} '.format(summary)
    linkedin_urls = ''
    for li_url in row['linkedin_url']:
        if li_url is not None:
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
    print(
        '[{}] Running hs_weekly_affinity_{}.py...'.format(
            datetime.now(), SPC_GEO.lower()
        )
    )
    conn = cnx.Cnx

    # pull data
    print('[{}] Getting data...'.format(datetime.now()))
    raw_hs_weekly = pd.read_sql_query(hs_weekly_query, conn)
    print('[{}] Done pulling data. Rows: {}'.format(datetime.now(), len(raw_hs_weekly)))

    # exclude junk companies
    print('[{}] Excluding junk companies...'.format(datetime.now()))
    junk_id_df = pd.read_csv(JUNK_COMPANY_ID_FILEPATH)
    junk_ids = list(junk_id_df['company_id'])
    hs_weekly = raw_hs_weekly[~raw_hs_weekly['company_id'].isin(junk_ids)]
    print(
        '[{}] Done excluding junk companies. Rows: {}'.format(
            datetime.now(), len(hs_weekly)
        )
    )

    # only keep companies with non-zero scores, and not is_irrelevant
    print(
        '[{}] Excluding zero-score and irrelevant companies...'.format(datetime.now())
    )
    hs_weekly = hs_weekly[hs_weekly['is_irrelevant_hs'] == False]
    hs_weekly = hs_weekly[hs_weekly['hs_score_v2'] > 0]
    print(
        '[{}] Done excluding irrelevant companies. Rows: {}'.format(
            datetime.now(), len(hs_weekly)
        )
    )

    # dedupe by linkedin_url and keep latest last_scraped_at
    # remove last character of linkedin url if it is a slash
    print('[{}] Deduping by linkedin_url...'.format(datetime.now()))
    hs_weekly['linkedin_url'] = hs_weekly['linkedin_url'].str.rstrip('/')
    hs_weekly_deduped = hs_weekly.sort_values(
        'last_scraped_at', ascending=False
    ).drop_duplicates('linkedin_url')
    print(
        '[{}] Done deduping by linkedin_url. Rows: {}'.format(
            datetime.now(), len(hs_weekly_deduped)
        )
    )

    print('[{}] Assembling metadata...'.format(datetime.now()))
    person_score_descriptions = pd.read_sql_query(person_score_description, conn)
    hs_weekly_with_descriptions = hs_weekly_deduped.merge(
        person_score_descriptions, on='person_id', how='left'
    )
    hs_weekly_with_descriptions['description'] = hs_weekly_with_descriptions[
        'description'
    ].replace(np.nan, None)

    # dedupe by full_name and company_id
    print('[{}] Deduping by full_name...'.format(datetime.now()))
    hs_weekly_with_descriptions_deduped = hs_weekly_with_descriptions.sort_values(
        'last_scraped_at', ascending=False
    ).drop_duplicates(['full_name', 'company_id'])

    # group by company
    print('[{}] Grouping by company...'.format(datetime.now()))
    hs_weekly_companies = (
        hs_weekly_with_descriptions_deduped.groupby(
            [
                'company_id',
                'hs_score_v2',
                'is_sweetspot_company',
                'is_traffic_priority',
                'founder_score_mean',
                'primary_url',
                'company_name',
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

    # format readable output
    print('[{}] Formatting founder col output...'.format(datetime.now()))
    # split up founders into cols
    founder_cols = pd.DataFrame(
        hs_weekly_companies['linkedin_url'].tolist(), index=hs_weekly_companies.index
    ).add_prefix('founder_')
    hs_weekly_founder_cols = pd.concat([hs_weekly_companies, founder_cols], axis=1)
    hs_weekly_fname = (
        _UPLOADS_DIR
        + '/top_20_hs_weekly_{}_{}.csv'.format(SPC_GEO, CURRENT_DATE).lower()
    )
    hs_weekly_founder_cols.sort_values('hs_score_v2', ascending=False).head(20).to_csv(
        hs_weekly_fname
    )

    # format afffinity upload
    print('[{}] Formatting affinity upload...'.format(datetime.now()))
    affinity_upload = (
        hs_weekly_companies[
            [
                'company_name',
                'primary_url',
                'linkedin_url',
                'hs_score_v2',
                'is_sweetspot_company',
                'is_traffic_priority',
                'founder_score_mean',
                'company_id',
                'full_name',
                'description',
            ]
        ]
        .sort_values('hs_score_v2', ascending=False)
        .head(40)
    )
    affinity_upload['note'] = affinity_upload.apply(create_note_string, axis=1)
    affinity_upload_final = affinity_upload[['company_name', 'primary_url', 'note']]
    affinity_upload_final.columns = [
        'Organization Name',
        'Organization Website',
        'Notes',
    ]
    affinity_upload_final['Owners'] = 'Roy Kimchi <roy@squarepegcap.com>'
    affinity_upload_final['Status'] = 'Haystack Review'
    affinity_upload_final['Referral Category'] = 'Haystack'
    # keep only companies with Organization Website
    affinity_upload_final = affinity_upload_final[
        affinity_upload_final['Organization Website'].notna()
    ]
    affinity_upload_fname = (
        _UPLOADS_DIR
        + '/affinity_upload_{}_{}.csv'.format(SPC_GEO, CURRENT_DATE).lower()
    )
    affinity_upload_final.to_csv(affinity_upload_fname, index=False)

    print('[{}] Done!'.format(datetime.now()))
