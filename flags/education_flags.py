import pandas as pd
from datetime import datetime
import re
from context import cnx
import warnings

# FLAG PATTERNS
phd_regex = re.compile(r'ph\.?d\.?|doctorate', re.IGNORECASE)
masters_regex = re.compile(r'\bm\.?s\.?\b|master|\bmba\b', re.IGNORECASE)
irrelevant_regex = re.compile(r'online|bootcamp|certificat|diploma', re.IGNORECASE)

if __name__ == '__main__':
    cnx = cnx.Cnx
    warnings.filterwarnings(action='ignore', category=UserWarning)

    # pull all educations
    print('[{}] Pulling educations'.format(datetime.now()))
    raw_educations_query = '''
        select 
            education_id
            , degree_name
        from educations;
        '''
    raw_educations = pd.read_sql_query(raw_educations_query, cnx)
    print(
        '[{}] Done pulling educations. Pulled: {}'.format(
            datetime.now(), len(raw_educations)
        )
    )

    # create flags
    print('[{}] Creating flags'.format(datetime.now()))
    educations = raw_educations.copy()
    educations['is_phd'] = educations['degree_name'].str.contains(phd_regex)
    educations['is_masters'] = educations['degree_name'].str.contains(masters_regex)
    educations['is_irrelevant'] = educations['degree_name'].str.contains(
        irrelevant_regex
    )
    print('[{}] Done creating flags'.format(datetime.now()))

    # write to db
    print('[{}] Writing to db'.format(datetime.now()))
    educations['generated_at'] = datetime.now()
    write_res = educations.to_sql(
        'education_flags', cnx, if_exists='replace', index=False, schema='score_v2'
    )
    print('[{}] Done writing to db'.format(datetime.now()))
