import pandas as pd
from datetime import datetime
from context import cnx

COMPANY_LIST_PATH = '/Users/kai/repositories/spc/haystack/haystack-score-v2/role_score/company_list_sea.csv'

roles_query = '''
    select
         r.role_title
        , r.linkedin_role_description
        , r.role_start
        , r.role_end
        , f.*
        , c."name" as company_name
    from roles r
    left join score_v2.role_flags f on f.role_id = r.role_id
    left join companies c on c.company_id = r.company_id
    left join person_locations l on l.person_id = r.person_id
    where l.spc_geo = 'SEA';
'''


def calc_role_score(row):
    role_score = 0
    # calculate tenure of role
    if row['tenure'] < 365:
        return 0

    if row['is_tier_company'] and row['seniority'] != 'junior':
        role_score += 1

    if row['seniority'] in ['exec', 'senior']:
        role_score *= 2

    return role_score


if __name__ == '__main__':
    print('[{}] Starting role_score_sea.py'.format(datetime.now()))
    conn = cnx.Cnx

    # pull roles with flags and company names / domains
    print('[{}] Pulling roles'.format(datetime.now()))
    raw_roles = pd.read_sql_query(roles_query, conn)
    print('[{}] Done pulling roles. Pulled: {}'.format(datetime.now(), len(raw_roles)))
    roles = raw_roles.copy()

    print('[{}] Cleaning data'.format(datetime.now()))

    # calculate tenure of role
    roles['role_start'].fillna(datetime.now(), inplace=True)
    roles['role_end'].fillna(datetime.now(), inplace=True)
    roles['tenure'] = (roles['role_end'] - roles['role_start']).dt.days

    # check if company_name is in tier list
    company_df = pd.read_csv(COMPANY_LIST_PATH)
    company_list = company_df['company_name'].str.lower().tolist()
    roles['is_tier_company'] = roles['company_name'].str.lower().isin(company_list)

    print('[{}] Calculating role score'.format(datetime.now()))
    roles['role_score'] = roles.apply(calc_role_score, axis=1)
    to_write = roles[['role_id', 'role_score']]

    # write to db
    print('[{}] Writing to db'.format(datetime.now()))
    to_write['generated_at'] = datetime.now()
    write_res = to_write.to_sql(
        'role_scores', conn, if_exists='replace', index=False, schema='score_v2'
    )
    print('[{}] Done writing to db'.format(datetime.now()))
