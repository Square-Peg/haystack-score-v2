import pandas as pd
from datetime import datetime
import re
from db import cnx
import warnings

# FLAG PATTERNS
founder_regex = re.compile(r'founder|cofounder|founding', re.IGNORECASE)
csuite_regex = re.compile(
    r'chief [a-z]* officer|chief of (data|engineering|product|staff)|\bc[etodfi]o\b',
    re.IGNORECASE,
)
stealth_regex = re.compile(
    r'stealth|something new|something exciting|something great', re.IGNORECASE
)
irrelevant_role_title_regex = re.compile(
    r'academy|ambassador|agent|advisor|boutique|case team|club|consult|fellow|festival|film|freelanc|google for startups|self[-\s]?employ|stage|theatre|ment[ee|or]|organi[s|z]er|partner|part[-\s]?time|program|venture|[executive|exec|personal] assistant|(stage|event|production|programme|program|office) manag',
    re.IGNORECASE,
)
irrelevant_role_description_regex = re.compile(
    r'advisor|ambassador|boutique|consult|freelanc|google for startups|self[-\s]?employ|part[-\s]?time',
    re.IGNORECASE,
)


# SENIORITY PATTERNS
junior_regex = re.compile(r'student|intern|scholar|junior|\bjr\b', re.IGNORECASE)
senior_regex = re.compile(
    r'senior|\bsr\b|\bsnr\b|lead|director|head|president|manager', re.IGNORECASE
)


# FUNCTIONS
def get_title_seniority(row):
    if row['is_founder'] or row['is_csuite']:
        return 'exec'

    if row['role_title']:
        if re.search(junior_regex, row['role_title']):
            return 'junior'
        elif re.search(senior_regex, row['role_title']) and not re.search(
            'product', row['role_title'], re.IGNORECASE
        ):
            return 'senior'
    return 'other'


if __name__ == '__main__':
    print('[{}] Starting role_flags.py'.format(datetime.now()))

    cnx = cnx.Cnx
    warnings.filterwarnings(action='ignore', category=UserWarning)

    # pull all roles
    print('[{}] Pulling roles'.format(datetime.now()))
    raw_roles_query = '''
        select 
            role_id
            , role_title
            , linkedin_role_description
        from roles;
        '''
    raw_roles = pd.read_sql_query(raw_roles_query, cnx)
    print('[{}] Done pulling roles. Pulled: {}'.format(datetime.now(), len(raw_roles)))

    # create flags
    print('[{}] Creating flags'.format(datetime.now()))
    roles = raw_roles.copy()
    roles['is_founder'] = roles['role_title'].str.contains(founder_regex)
    roles['is_csuite'] = roles['role_title'].str.contains(csuite_regex)
    roles['is_stealth'] = roles['role_title'].str.contains(stealth_regex)
    roles['is_irrelevant_role'] = roles['role_title'].str.contains(
        irrelevant_role_title_regex
    ) | roles['linkedin_role_description'].str.contains(
        irrelevant_role_description_regex
    )
    print('[{}] Done creating flags'.format(datetime.now()))

    # infer seniority
    print('[{}] Inferring seniority'.format(datetime.now()))
    roles['seniority'] = roles.apply(get_title_seniority, axis=1)
    print('[{}] Done inferring seniority'.format(datetime.now()))

    # write to db
    print('[{}] Writing to db'.format(datetime.now()))
    roles['generated_at'] = datetime.now()
    write_res = roles.to_sql(
        'role_flags', cnx, if_exists='replace', index=False, schema='score_v2'
    )
    print('[{}] Done writing to db, rows: {}'.format(datetime.now(), write_res))
