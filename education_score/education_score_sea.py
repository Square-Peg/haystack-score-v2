import pandas as pd
from datetime import datetime
from context import cnx

SCHOOL_LIST_PATH = '/Users/kai/repositories/spc/haystack/haystack-score-v2/education_score/school_list_sea.csv'

educations_query = '''
    select
         e.degree_name
        , f.*
        , s."name" as school_name
    from educations e
    left join score_v2.education_flags f on f.education_id = e.education_id
    left join schools s on s.school_id = e.school_id
    left join person_locations l on l.person_id = e.person_id
    where e.person_id in (
        select distinct person_id
        from score_v2.person_locations
        where spc_geo = 'SEA'
        )
'''


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
    print('[{}] Starting education_score_sea.py'.format(datetime.now()))
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
    to_write = educations[['education_id', 'education_score']]

    # write to db
    print('[{}] Writing to db'.format(datetime.now()))
    to_write['generated_at'] = datetime.now()
    write_res = to_write.to_sql(
        'education_scores', conn, if_exists='replace', index=False, schema='score_v2'
    )
    print('[{}] Done writing to db'.format(datetime.now()))
