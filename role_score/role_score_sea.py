import pandas as pd
from datetime import datetime
from context import cnx
from sqlalchemy.orm import sessionmaker

SPC_GEO = 'SEA'
COMPANY_LIST_PATH = '/Users/kai/repositories/spc/haystack/haystack-score-v2/data/company_list_{}.csv'.format(
    SPC_GEO.lower()
)

roles_query = '''
    select
         r.role_title
        , r.linkedin_role_description
        , r.role_start
        , r.role_end
        , r.role_id
        , f.is_founder
        , f.is_csuite
        , f.is_stealth
        , f.is_irrelevant_role
        , f.seniority
        , c."name" as company_name
    from roles r
    left join score_v2.role_flags f on f.role_id = r.role_id
    left join companies c on c.company_id = r.company_id
    where r.person_id in (
        select distinct person_id
        from score_v2.person_locations
        where spc_geo = '{}'
        )
'''.format(
    SPC_GEO
)

delete_role_scores_query = '''
    DO
    $$
    BEGIN
        IF EXISTS (
            SELECT FROM information_schema.tables 
            WHERE  table_schema = 'score_v2'
            AND    table_name   = 'role_scores'
        )
        THEN 
            DELETE FROM score_v2.role_scores WHERE spc_geo = '{}';
        END IF;
    END
    $$
'''.format(
    SPC_GEO
)


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
    print('[{}] Starting role_score_{}.py'.format(datetime.now(), SPC_GEO.lower()))
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
    print('[{}] Deleting old {} role scores'.format(datetime.now(), SPC_GEO))
    try:
        session = sessionmaker(bind=conn)()
        session.execute(delete_role_scores_query)
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()

    print('[{}] Writing to db'.format(datetime.now()))
    to_write['generated_at'] = datetime.now()
    to_write['spc_geo'] = SPC_GEO
    write_res = to_write.to_sql(
        'role_scores', conn, if_exists='append', index=False, schema='score_v2'
    )
    print('[{}] Done writing to db'.format(datetime.now()))
