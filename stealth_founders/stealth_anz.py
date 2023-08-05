import pandas as pd
from datetime import datetime, timedelta
from context import cnx
import numpy as np
from sqlalchemy.orm import sessionmaker

# INITIALISE CONSTANTS
SPC_GEO = 'SEA'
AFFINITY_OWNERS = 'Frederick Ng <fred@squarepegcap.com>'
_UPLOADS_DIR = '/Users/kai/repositories/spc/haystack/haystack-score-v2/_stealth_uploads'
_EXCLUSION_DIR = (
    '/Users/kai/repositories/spc/haystack/haystack-score-v2/_stealth_exclusions'
)

current_date = datetime.now()
days_to_subtract = current_date.weekday()

CURRENT_DATE_STRING = current_date.strftime('%Y%m%d')
CURRENT_DATE_WITH_DASH_STRING = current_date.strftime('%Y-%m-%d')
WEEK_START_DATE_WITH_DASH_STRING = (
    current_date - timedelta(days=days_to_subtract)
).strftime('%Y-%m-%d')

# SQL QUERIES
hs_stealth_query = '''
    select 
        p.linkedin_url
        , p.full_name
        , r.role_start
        , p.linkedin_summary
        , r.linkedin_role_description
        , ps.description
        , ps.score
        , r.last_scraped_at
        , ps.spc_geo
        , ps.person_id
    from score_v2.person_scores ps 
        left join roles r on r.person_id = ps.person_id
        left join companies c on c.company_id = r.company_id
        left join persons p on p.person_id = ps.person_id
        where r.is_stealth_role = TRUE
        and r.role_end is NULL
        and r.last_scraped_at > '{}'
        and score > 0
        and ps.spc_geo = '{}' 
    '''.format(
    WEEK_START_DATE_WITH_DASH_STRING, SPC_GEO
)

crm_name_query = '''
    select 
        "name" as organisation_name 
    from 
        crm_exports crme 
    where 
        crme."name" ilike '%%stealth%%'
'''


# helper functions
def truncate_linkedin_description(description):
    if description and (description is not np.nan):
        if len(description) > 200:
            return description[:200] + '...'
        else:
            return description
    else:
        return 'None'


def create_stealth_note_string(row):
    note = '''Haystack Summary: {founder_summary}
Stealth Start Date: {role_start}
Stealth Role Description: {role_description}
LinkedIn Description: {linkedin_summary}
LinkedIn URL: {linkedin_url}

------

Founder Score: {founder_score:.2f}
Haystack Person ID: {person_id}
Date Generated: {date_generated}
'''

    founder_summary = row['description'] if row['description'] is not np.nan else 'None'
    role_start = (
        row['role_start'].strftime('%b %Y')
        if row['role_start'] is not np.nan
        else 'None'
    )
    linkedin_summary = truncate_linkedin_description(row['linkedin_summary'])
    role_description = truncate_linkedin_description(row['linkedin_role_description'])
    linkedin_url = row['linkedin_url'] if row['linkedin_url'] is not np.nan else 'None'

    note = note.format(
        founder_summary=founder_summary,
        role_start=role_start,
        linkedin_summary=linkedin_summary,
        role_description=role_description,
        linkedin_url=linkedin_url,
        founder_score=row['score'],
        person_id=row['person_id'],
        date_generated=CURRENT_DATE_WITH_DASH_STRING,
    )

    return note


def create_name(row):
    if row['full_name']:
        # strip all symbols and whitespace from full name
        name = ''.join(e for e in row['full_name'] if e.isalnum())
        return 'StealthCo_' + name
    else:
        return 'StealthCo_' + str(row['person_id'])


# SCRIPT
if __name__ == '__main__':
    print('[{}] Running stealth_{}.py...'.format(datetime.now(), SPC_GEO.lower()))
    print('[{}] Current date: {}'.format(datetime.now(), CURRENT_DATE_STRING))
    print(
        '[{}] **Week start date**: {}'.format(
            datetime.now(), WEEK_START_DATE_WITH_DASH_STRING
        )
    )

    conn = cnx.Cnx

    # pull sea stealth founders
    print('[{}] Getting data...'.format(datetime.now()))
    raw_stealth = pd.read_sql_query(hs_stealth_query, conn)
    print('[{}] Done pulling data. Rows: {}'.format(datetime.now(), len(raw_stealth)))

    # generate notes
    print('[{}] Generating notes...'.format(datetime.now()))
    stealth = raw_stealth.copy()
    stealth['notes'] = stealth.apply(create_stealth_note_string, axis=1)
    print('[{}] Done generating notes.'.format(datetime.now()))

    # format for affinity upload
    print('[{}] Formatting for Affinity...'.format(datetime.now()))
    stealth['affinity_name'] = stealth.apply(create_name, axis=1)
    stealth_upload = stealth[['affinity_name', 'notes']]
    stealth_upload.columns = ['Organization Name', 'Notes']
    stealth_upload['match_name'] = stealth['full_name'].fillna(
        stealth['person_id'].astype(str)
    )
    stealth_upload['Owners'] = AFFINITY_OWNERS
    stealth_upload['Organization Website'] = None
    stealth_upload['Status'] = 'Haystack Review'
    stealth_upload['Referral Category'] = 'Haystack'
    print('[{}] Done formatting for Affinity.'.format(datetime.now()))

    # exclude stealth companies that are already in the CRM
    print('[{}] Excluding stealth companies already in CRM...'.format(datetime.now()))
    # strip all symbols, whitespace, and lowercase from crm names
    crm_names = pd.read_sql_query(crm_name_query, conn)
    crm_names['organisation_name'] = crm_names['organisation_name'].apply(
        lambda x: ''.join(e for e in x if e.isalnum()).lower()
    )
    crm_name_string = crm_names['organisation_name'].tolist()
    crm_name_string = '|'.join(crm_name_string)

    # check if full_name is a substring of any crm name
    stealth_upload['match_name'] = stealth_upload['match_name'].apply(
        lambda x: ''.join(e for e in x if e.isalnum()).lower()
    )
    stealth_upload['to_exclude'] = stealth_upload['match_name'].apply(
        lambda x: x in crm_name_string
    )

    stealth_upload_final = stealth_upload[~stealth_upload['to_exclude']]
    stealth_upload_final = stealth_upload_final.drop(
        ['to_exclude', 'match_name'], axis=1
    )
    stealth_exclusions = stealth_upload[stealth_upload['to_exclude']]
    excluded_count = len(stealth_exclusions)
    print('[{}] Done excluding. Excluded: {}'.format(datetime.now(), excluded_count))

    # save as csv
    print('[{}] Saving as csv...'.format(datetime.now()))
    stealth_upload_final.to_csv(
        '{}/affinity_upload_stealth_{}_{}.csv'.format(
            _UPLOADS_DIR, SPC_GEO.lower(), CURRENT_DATE_STRING
        ),
        index=False,
    )
    stealth_exclusions.to_csv(
        '{}/affinity_upload_stealth_{}_excluded_{}.csv'.format(
            _EXCLUSION_DIR, SPC_GEO.lower(), CURRENT_DATE_STRING
        ),
        index=False,
    )
    print('[{}] Done saving as csv.'.format(datetime.now()))
