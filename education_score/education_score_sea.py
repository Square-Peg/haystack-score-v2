import pandas as pd
from datetime import datetime
from context import cnx
from sqlalchemy.orm import sessionmaker

SPC_GEO = 'SEA'
SCHOOL_LIST_PATH = '/Users/kai/repositories/spc/haystack/haystack-score-v2/data/school_list_{}.csv'.format(
    SPC_GEO.lower()
)


educations_query = '''
    select
         e.degree_name
        , e.education_id
        , ef.is_phd
        , ef.is_masters
        , ef.is_irrelevant
        , s."name" as school_name
    from educations e
    left join score_v2.education_flags ef on ef.education_id = e.education_id
    left join schools s on s.school_id = e.school_id
    where e.person_id in (
        select distinct person_id
        from score_v2.person_locations
        where spc_geo = '{}'
        )
'''.format(
    SPC_GEO
)

delete_education_score_query = '''
    DO
    $$
    BEGIN
        IF EXISTS (
            SELECT FROM information_schema.tables 
            WHERE  table_schema = 'score_v2'
            AND    table_name   = 'education_scores'
        )
        THEN 
            DELETE FROM score_v2.education_scores WHERE spc_geo = '{}';
        END IF;
    END
    $$
'''.format(
    SPC_GEO
)


def calc_education_score(row):
    if row['is_irrelevant']:
        return 0

    education_score = 0
    if row['is_tier_school']:
        education_score += 1

    if row['is_phd'] or row['is_masters']:
        education_score *= 2

    return education_score


if __name__ == '__main__':
    print('[{}] Starting education_score_{}.py'.format(datetime.now(), SPC_GEO))
    conn = cnx.Cnx

    # pull educations with flags and school names
    print('[{}] Pulling educations'.format(datetime.now()))
    raw_educations = pd.read_sql_query(educations_query, conn)
    print(
        '[{}] Done pulling educations. Pulled: {}'.format(
            datetime.now(), len(raw_educations)
        )
    )
    educations = raw_educations.copy()

    print('[{}] Cleaning data'.format(datetime.now()))

    # check if school_name is in tier list
    school_df = pd.read_csv(SCHOOL_LIST_PATH)
    school_list = school_df['school_name'].str.lower().tolist()
    educations['is_tier_school'] = (
        educations['school_name'].str.lower().isin(school_list)
    )

    print('[{}] Calculating education score'.format(datetime.now()))
    educations['education_score'] = educations.apply(calc_education_score, axis=1)

    # write to db
    print('[{}] Deleting old {} education scores'.format(datetime.now(), SPC_GEO))
    try:
        session = sessionmaker(bind=conn)()
        session.execute(delete_education_score_query)
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()

    print('[{}] Writing to db'.format(datetime.now()))
    to_write = educations[['education_id', 'education_score']]
    to_write['generated_at'] = datetime.now()
    to_write['spc_geo'] = SPC_GEO
    write_res = to_write.to_sql(
        'education_scores', conn, if_exists='append', index=False, schema='score_v2'
    )
    print('[{}] Done writing to db'.format(datetime.now()))
