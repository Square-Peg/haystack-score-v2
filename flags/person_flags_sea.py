import pandas as pd
import numpy as np
from datetime import datetime
from context import cnx

# pull all people with their educations
persons_query = '''
    select 
        p.person_id
        , ef.is_masters
        , ef.is_phd
        , e.degree_end
    from persons p
    left join educations e on e.person_id = p.person_id
    left join score_v2.education_flags ef on ef.education_id = e.education_id
'''

if __name__ == '__main__':
    print('[{}] Starting...'.format(datetime.now()))
    # Create the connection
    conn = cnx.Cnx
    # pull all people with their educations
    raw_persons = pd.read_sql_query(persons_query, conn)

    persons = raw_persons.copy()

    # 'currently undergrad' is if the degree_end is between 2023 and 2026, and is not a masters or phd
    persons['currently_undergrad'] = np.where(
        (persons['degree_end'] >= datetime(2023, 1, 1))
        & (persons['degree_end'] <= datetime(2026, 12, 31))
        & (persons['is_masters'] == 0)
        & (persons['is_phd'] == 0),
        True,
        False,
    )

    # write to db
    print('[{}] Writing to db...'.format(datetime.now()))
    to_write = persons[['person_id', 'currently_undergrad']]
    to_write['generated_at'] = datetime.now()
    to_write.to_sql(
        'person_flags', conn, if_exists='replace', index=False, schema='score_v2'
    )
    print('[{}] Done!'.format(datetime.now()))
