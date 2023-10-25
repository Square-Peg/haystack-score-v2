import pandas as pd
from datetime import datetime
import re
from context import cnx
import warnings

# FLAG PATTERNS
irrelevant_name_regex = re.compile(
    r'academy|agency|australia|accelerat|\bangel\b|boutique|capital|club|chapter|consult|digital|festival|film|freelanc|\blaw\b|lawyer|legal|litigat|management|marketing|media|partner|project|studio|scholar|start[-\s]?up|student|self[-\s]?employ|\bwine\b|venture|&|whiskey|whisky|non[-\s]?profit|podcast|\bngo\b|\bphilanthro\b|key opinion|influencer|affiliate',
    re.IGNORECASE,
)
irrelevant_domain_regex = re.compile(r'.gov|studio|substack', re.IGNORECASE)
irrelevant_industry_regex = re.compile(
    r'Marketing Services|Non-profit Organizations|Professional Training and Coaching',
    re.IGNORECASE,
)

## nonprofit podcast


if __name__ == '__main__':
    cnx = cnx.Cnx
    warnings.filterwarnings(action='ignore', category=UserWarning)

    # pull all companies
    print('[{}] Pulling companies'.format(datetime.now()))
    raw_companies_query = '''
        select
            company_id
            , name as company_name
            , primary_url as company_primary_url
            , industry as company_industry
        from companies;
        '''
    raw_companies = pd.read_sql_query(raw_companies_query, cnx)
    print(
        '[{}] Done pulling companies. Pulled: {}'.format(
            datetime.now(), len(raw_companies)
        )
    )

    # create flags
    print('[{}] Creating flags'.format(datetime.now()))
    companies = raw_companies.copy()
    companies['is_irrelevant'] = (
        companies['company_name'].str.contains(irrelevant_name_regex)
        | companies['company_primary_url'].str.contains(irrelevant_domain_regex)
        | companies['company_industry'].str.contains(irrelevant_industry_regex)
    )
    print('[{}] Done creating flags'.format(datetime.now()))

    # write to db
    print('[{}] Writing to db'.format(datetime.now()))
    companies['generated_at'] = datetime.now()
    write_res = companies.to_sql(
        'company_flags', cnx, if_exists='replace', index=False, schema='score_v2'
    )
    print('[{}] Done writing to db'.format(datetime.now()))
