import os
import warnings

import pandas as pd
from context import cnx

warnings.simplefilter(action='ignore', category=FutureWarning)

conn = cnx.Cnx


# Constants
CURRENT_TS = pd.to_datetime('today').strftime('%Y%m%d_%H_%M_%S')
CURRENT_DATE = pd.to_datetime('today').strftime('%Y-%m-%d')
HS_LOOKBACK_INTERVAL = '14 days'
HS_OUTPUT_PATH = '/Users/kai/repositories/spc/haystack/haystack-score-v2/_sw_pull_list/hs_sw_pull_{}.csv'.format(
    CURRENT_TS
)
GP_OUTPUT_PATH = '/Users/kai/repositories/spc/haystack/haystack-score-v2/_sw_pull_list/gp_sw_pull_{}.csv'.format(
    CURRENT_TS
)

print('Starting script...')
print('HS_LOOKBACK_INTERVAL: {}'.format(HS_LOOKBACK_INTERVAL))
print('HS_OUTPUT_PATH: {}'.format(HS_OUTPUT_PATH))
print('GP_OUTPUT_PATH: {}'.format(GP_OUTPUT_PATH))

# Pull data from haystack
print('Pulling data from Haystack...')
print(
    "SQL date range: '{}'::date - INTERVAL '{}'".format(
        CURRENT_DATE, HS_LOOKBACK_INTERVAL
    )
)
raw_haystack = pd.read_sql(
    '''
select primary_url as domain, c.company_id
from roles r
left join companies c on c.company_id = r.company_id
where r.last_scraped_at > '{}'::date - INTERVAL '{}'
and (r.role_title ilike '%%found%%' or r.role_title ilike '%%ceo%%' or r.role_title ilike '%%stealth%%' or c."name" ilike '%%stealth%%')
and primary_url is not null;
'''.format(
        CURRENT_DATE, HS_LOOKBACK_INTERVAL
    ),
    conn,
)

# Dedupe data from haystack
haystack = raw_haystack.drop_duplicates(subset=['domain']).copy()
haystack['list_generated_at'] = pd.to_datetime('now')
haystack.rename({'company_primary_url': 'domain'}, axis=1, inplace=True)
print('Done pulling Haystack data: {} rows'.format(len(haystack)))

# Pull data from global pipeline
print('Pulling data from Global Pipeline...')
raw_global_pipeline = pd.read_sql(
    '''
select 
    affinity_organisation_id
    , website as company_primary_url
from crm_exports crm
where true
and affinity_list = 'global_pipeline' 
and date_added_gp > '2019-01-01'
and website is not null
''',
    conn,
)

# Pull data from portfolio list
print('Pulling data from Portfolio...')
raw_portfolio = pd.read_sql(
    '''
select 
    affinity_organisation_id
    , website as company_primary_url
from crm_exports crm
where true
and affinity_list = 'portfolio' 
and website is not null
''',
    conn,
)

# Dedupe data from affinity
global_pipeline = raw_global_pipeline.drop_duplicates(
    subset=['company_primary_url']
).copy()
global_pipeline['list_generated_at'] = pd.to_datetime('now')
global_pipeline.rename({'company_primary_url': 'domain'}, axis=1, inplace=True)
print('Done pulling Global Pipeline data: {} rows'.format(len(global_pipeline)))

# Save lists to csv
print('Done pulling data. Saving to csv...')
haystack.to_csv(HS_OUTPUT_PATH, index=False)
global_pipeline.to_csv(GP_OUTPUT_PATH, index=False)
print('Done saving csvs.')
