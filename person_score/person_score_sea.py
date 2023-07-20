import pandas as pd
from datetime import datetime
from context import cnx
import numpy as np
from sqlalchemy.orm import sessionmaker

SPC_GEO = 'SEA'

person_ids_query = '''
    select distinct person_id
    from score_v2.person_locations
    where spc_geo = '{}'
'''.format(
    SPC_GEO
)

education_scores_query = '''
    select
        p.person_id
        , es.education_score
        , e.degree_name
        , s."name" as school_name
    from persons p
        left join educations e on e.person_id = p.person_id
        left join schools s on s.school_id = e.school_id
        left join score_v2.education_scores es on es.education_id = e.education_id
    where p.person_id in (
        select distinct person_id
        from score_v2.person_locations
        where spc_geo = '{}'
        )
'''.format(
    SPC_GEO
)

role_scores_query = '''
    select
        p.person_id
        , rs.role_score
        , r.role_title
        , c."name" as company_name
        , c.company_id
    from persons p
        left join roles r on r.person_id = p.person_id
        left join companies c on c.company_id = r.company_id
        left join score_v2.role_scores rs on rs.role_id = r.role_id
    where p.person_id in (
        select distinct person_id
        from score_v2.person_locations
        where spc_geo = '{}'
        )
'''.format(
    SPC_GEO
)

delete_person_score_query = '''
    DO
    $$
    BEGIN
        IF EXISTS (
            SELECT FROM information_schema.tables 
            WHERE  table_schema = 'score_v2'
            AND    table_name   = 'person_scores'
        )
        THEN 
            DELETE FROM score_v2.person_scores WHERE spc_geo = '{}';
        END IF;
    END
    $$
'''.format(
    SPC_GEO
)

if __name__ == '__main__':
    print('[{}] Starting person_score_{}.py'.format(datetime.now(), SPC_GEO.lower()))
    conn = cnx.Cnx

    # pull data
    print('[{}] Pulling role scores'.format(datetime.now()))
    raw_role_scores = pd.read_sql_query(role_scores_query, conn)
    print(
        '[{}] Done pulling role scores. Pulled: {}'.format(
            datetime.now(), len(raw_role_scores)
        )
    )
    print('[{}] Pulling education scores'.format(datetime.now()))
    raw_education_scores = pd.read_sql_query(education_scores_query, conn)
    print(
        '[{}] Done pulling education scores. Pulled: {}'.format(
            datetime.now(), len(raw_education_scores)
        )
    )

    # prep data
    print('[{}] Cleaning data'.format(datetime.now()))
    role_scores = raw_role_scores.copy()
    education_scores = raw_education_scores.copy()

    education_scores_dropped = education_scores.dropna(subset=['education_score'])
    education_stores_filled = education_scores_dropped.replace(
        [None, '', np.nan], 'N/A'
    )
    education_stores_filled['description'] = (
        education_stores_filled['degree_name']
        + ' @ '
        + education_stores_filled['school_name']
    )
    education_stores_filled.rename(columns={'education_score': 'score'}, inplace=True)

    # if person has more than one role in the same company, then keep the highest score only
    role_scores_sorted = role_scores.sort_values(
        by=['person_id', 'company_id', 'role_score']
    )
    role_scores_deduped = role_scores_sorted.drop_duplicates(
        subset=['person_id', 'company_id'], keep='first'
    )
    role_scores_dropped = role_scores_deduped.dropna(subset=['role_score'])
    role_scores_filled = role_scores_dropped.replace([None, '', np.nan], 'N/A')
    role_scores_filled['description'] = (
        role_scores_filled['role_title'] + ' @ ' + role_scores_filled['company_name']
    )
    role_scores_filled.rename(columns={'role_score': 'score'}, inplace=True)

    all_scores = pd.concat(
        [
            role_scores_filled[['person_id', 'score', 'description']],
            education_stores_filled[['person_id', 'score', 'description']],
        ],
        ignore_index=True,
    )
    all_scores = all_scores[all_scores['score'] > 0]

    # Group and generate person scores
    print('[{}] Generating person scores'.format(datetime.now()))

    person_scores = (
        all_scores.groupby('person_id')
        .agg({'score': 'sum', 'description': lambda x: list(x)})
        .reset_index()
    )

    # make description column into a string
    person_scores['description'] = person_scores['description'].apply(
        lambda x: ', '.join(x)
    )

    # Join back with all geo persons
    all_persons = pd.read_sql_query(person_ids_query, conn)

    all_persons = all_persons.merge(person_scores, on='person_id', how='left')
    all_persons_filled = all_persons.copy()
    all_persons_filled['score'] = all_persons_filled['score'].fillna(0)

    # write to db
    print('[{}] Deleting existing {} person scores'.format(datetime.now(), SPC_GEO))
    try:
        session = sessionmaker(bind=conn)()
        session.execute(delete_person_score_query)
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()

    print('[{}] Writing to db'.format(datetime.now()))
    all_persons_filled['generated_at'] = datetime.now()
    all_persons_filled['spc_geo'] = SPC_GEO
    all_persons_filled.to_sql(
        'person_scores', conn, if_exists='append', schema='score_v2', index=False
    )
    print('[{}] Done writing to db'.format(datetime.now()))
