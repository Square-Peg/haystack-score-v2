import pandas as pd
from datetime import datetime
from context import cnx
import re

companies_query = '''
    select distinct 
        company_id
        , c."name" as company_name
        , primary_url as company_primary_url
    from companies c
'''

execs_query = '''
    select
          r.company_id
        , r.role_title
        , r.linkedin_role_description
        , p.linkedin_summary
    from roles r 
    left join persons p on p.person_id = r.person_id
    left join score_v2.role_flags rf on rf.role_id = r.role_id
    where rf.seniority = 'exec'
    and r.company_id is not null
'''

ss_exec_pattern = re.compile(
    r'\bai\b|\bAI\b|artificial intelligence|\bml\b|\bML\b|machine learning|deep learning|neural network|computer vision|natural language processing|\bnlp\b|\bNLP\b'
)
ai_io_pattern = re.compile(r'\.ai|\.io')
ai_pattern = re.compile(r'.*AI\b|\bai\b')


if __name__ == '__main__':
    conn = cnx.Cnx

    # Pull data
    print('[{}] Pulling companies...'.format(datetime.now()))
    raw_companies = pd.read_sql(companies_query, conn)
    print('[{}] Done pulling companies.'.format(datetime.now()))

    print('[{}] Pulling execs...'.format(datetime.now()))
    raw_execs = pd.read_sql(execs_query, conn)
    print('[{}] Done pulling execs.'.format(datetime.now()))

    print('[{}] Creating intermediate sweetspot flags...'.format(datetime.now()))
    # has_sweetspot_keywords
    companies = raw_companies.copy()
    # check company_primary_url for .ai or .io
    companies['has_ai_io_domain'] = companies['company_primary_url'].str.contains(
        ai_io_pattern
    )
    companies['has_ai_io_domain'] = companies['has_ai_io_domain'].fillna(False)

    # check company_name for 'AI' or 'ai'
    companies['has_ai_name'] = companies['company_name'].str.contains(ai_pattern)

    # has_sweetspot_exec
    execs = raw_execs.copy()
    execs_filled = execs.fillna('')
    company_execs = (
        execs_filled.groupby('company_id')
        .agg(
            {
                'role_title': lambda x: ' '.join(x),
                'linkedin_role_description': lambda x: ' '.join(x),
                'linkedin_summary': lambda x: ' '.join(x),
            }
        )
        .reset_index()
    )

    company_execs['has_sweetspot_exec'] = company_execs['role_title'].str.contains(
        ss_exec_pattern
    ) | company_execs['linkedin_role_description'].str.contains(ss_exec_pattern)

    companies_with_execs = companies.merge(
        company_execs[['company_id', 'has_sweetspot_exec']], on='company_id', how='left'
    )
    companies_with_execs['has_sweetspot_exec'] = companies_with_execs[
        'has_sweetspot_exec'
    ].fillna(False)
    print('[{}] Done creating intermediate sweetspot flags.'.format(datetime.now()))

    # creat final sweetspot flag
    print('[{}] Creating final sweetspot flag...'.format(datetime.now()))
    companies_with_execs['is_sweetspot_company'] = (
        companies_with_execs['has_ai_io_domain']
        | companies_with_execs['has_ai_name']
        | companies_with_execs['has_sweetspot_exec']
    )
    print('[{}] Done creating final sweetspot flag.'.format(datetime.now()))

    # write to db
    print('[{}] Writing to db...'.format(datetime.now()))
    to_write = companies_with_execs[
        [
            'company_id',
            'has_ai_io_domain',
            'has_ai_name',
            'has_sweetspot_exec',
            'is_sweetspot_company',
        ]
    ]

    to_write.to_sql(
        'company_sweetspot_flags',
        conn,
        if_exists='replace',
        index=False,
        schema='score_v2',
    )
    print('[{}] Done writing to db.'.format(datetime.now()))
