import pandas as pd
from datetime import datetime, timedelta
from context import cnx
import numpy as np

# INITIALISE CONSTANTS
SPC_GEO = 'ISR'
AFFINITY_OWNERS = 'Roy Kimchi <roy@squarepegcap.com>'
JUNK_COMPANY_ID_FILEPATH = (
    '/Users/kai/repositories/spc/haystack/haystack-score-v2/data/junk_company_ids.csv'
)
_UPLOADS_DIR = '/Users/kai/repositories/spc/haystack/haystack-score-v2/_uploads'

current_date = datetime.now()
days_to_subtract = current_date.weekday()

CURRENT_DATE_STRING = current_date.strftime('%Y%m%d')
CURRENT_DATE_WITH_DASH_STRING = current_date.strftime('%Y-%m-%d')
WEEK_START_DATE_WITH_DASH_STRING = (
    current_date - timedelta(days=days_to_subtract)
).strftime('%Y-%m-%d')

# SQL QUERIES
hs_weekly_query = '''
    select hs.*
    , c.primary_url
    , c."name" as company_name
    from 
    score_v2.haystack_scores hs
    left join companies c on c.company_id = hs.company_id
    where hs.hs_score_v2 is not null
    and c.last_scraped_at > '{}'
    and hs.spc_geo = '{}'
'''.format(
    WEEK_START_DATE_WITH_DASH_STRING, SPC_GEO
)

# SCRIPT
if __name__ == '__main__':
    print('[{}] Running hs_uploads_{}.py...'.format(datetime.now(), SPC_GEO.lower()))
    print('[{}] Current date: {}'.format(datetime.now(), CURRENT_DATE_STRING))
    print(
        '[{}] **Week start date**: {}'.format(
            datetime.now(), WEEK_START_DATE_WITH_DASH_STRING
        )
    )
    conn = cnx.Cnx

    # pull data
    print('[{}] Getting data...'.format(datetime.now()))
    raw_hs_weekly = pd.read_sql_query(hs_weekly_query, conn)
    print('[{}] Done pulling data. Rows: {}'.format(datetime.now(), len(raw_hs_weekly)))

    # only keep companies with non-zero scores, and not is_irrelevant
    print(
        '[{}] Excluding zero-score and irrelevant companies...'.format(datetime.now())
    )
    hs_weekly = raw_hs_weekly[raw_hs_weekly['is_irrelevant_hs'] == False]
    hs_weekly = hs_weekly[hs_weekly['hs_score_v2'] > 0]
    print(
        '[{}] Done excluding irrelevant companies. Rows: {}'.format(
            datetime.now(), len(hs_weekly)
        )
    )

    # format afffinity upload
    print('[{}] Formatting affinity upload...'.format(datetime.now()))
    affinity_upload = hs_weekly.sort_values('hs_score_v2', ascending=False).head(40)
    affinity_upload_final = affinity_upload[
        ['company_name', 'primary_url', 'notes', 'company_id']
    ]
    affinity_upload_final.columns = [
        'Organization Name',
        'Organization Website',
        'Notes',
        'company_id',
    ]
    affinity_upload_final['Owners'] = AFFINITY_OWNERS
    affinity_upload_final['Status'] = 'Haystack Review'
    affinity_upload_final['Referral Category'] = 'Haystack'
    # keep only companies with Organization Website
    affinity_upload_final = affinity_upload_final[
        affinity_upload_final['Organization Website'].notna()
    ]
    affinity_upload_fname = (
        _UPLOADS_DIR
        + '/affinity_upload_{}_{}.csv'.format(SPC_GEO, CURRENT_DATE_STRING).lower()
    )
    affinity_upload_final.to_csv(affinity_upload_fname, index=False)

    print('[{}] Done!'.format(datetime.now()))
