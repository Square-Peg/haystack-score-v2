import pandas as pd
import os
import sqlalchemy

from context import cnx
from tqdm import tqdm


spcGeoMapping = {
    'AUS': 'ANZ',  # australia
    'NZL': 'ANZ',  # new zealand
    'ISR': 'ISR',  # israel
    'SGP': 'SEA',  # singapore
    'MYS': 'SEA',  # malaysia
    'IDN': 'SEA',  # indonesia
    'PHL': 'SEA',  # philippines
    'THA': 'SEA',  # thailand
    'BRN': 'SEA',  # brunei
    'KHM': 'SEA',  # cambodia
    'LAO': 'SEA',  # laos
    'MMR': 'SEA',  # myanmar
    'VNM': 'SEA',  # vietnam
}

roleQuery = '''
    select
        p.person_id as person_id,
        p.full_name as full_name,
        p.country_code_alpha3 as person_country_code_alpha3,
        p.city as person_city,
        r.role_id as role_id,
        r.role_start as role_start,
        r.country_code_alpha3 as role_country_code_alpha3,
        r.city as role_city
    from v1.persons p
    left join v1.roles r on r.person_id = p.person_id
    '''

locationDbCols = [
    'person_id',
    'inferred_country_code_alpha3',
    'inferred_city',
    'location_metadata',
    'spc_geo',
    'spc_geo_metadata',
]


def inferPersonGeo(group: pd.DataFrame) -> pd.Series:
    group = group.sort_values(by='role_start', ascending=False)
    inferredLocation = {
        'inferred_country_code_alpha3': None,
        'inferred_city': None,
        'location_metadata': None,
        'spc_geo': None,
        'spc_geo_metadata': None,
    }
    # Try to infer first from person
    firstRow = group.iloc[0]
    if (
        pd.notna(firstRow['person_country_code_alpha3'])
        and firstRow['person_country_code_alpha3'] != ''
    ):
        inferredLocation['inferred_country_code_alpha3'] = firstRow[
            'person_country_code_alpha3'
        ]
        inferredLocation['inferred_city'] = firstRow['person_city']
        inferredLocation['location_metadata'] = {
            'source': 'person',
            'source_id': int(firstRow['person_id']),
        }
    else:
        # Otherwise, check for locations from roles
        for index, row in group.iterrows():
            if (
                pd.notna(row['role_country_code_alpha3'])
                and row['role_country_code_alpha3'] != ''
            ):
                inferredLocation['inferred_country_code_alpha3'] = row[
                    'role_country_code_alpha3'
                ]
                inferredLocation['inferred_city'] = row['role_city']
                inferredLocation['location_metadata'] = {
                    'source': 'role',
                    'source_id': int(row['role_id']),
                }
                break

    # Try to assign to SPC geo based on inferredLocation
    # if inferredLocation isn't one of the geos, then try to assign based on all their roles
    inferredGeo = spcGeoMapping.get(
        inferredLocation['inferred_country_code_alpha3'], None
    )
    if inferredGeo:
        inferredLocation['spc_geo'] = inferredGeo
        inferredLocation['spc_geo_metadata'] = inferredLocation['location_metadata']
    else:
        # We don't have to consider the person_country, because it would have been assigned above
        for index, row in group.iterrows():
            geoFromRole = spcGeoMapping.get(row['role_country_code_alpha3'], None)
            if geoFromRole:
                inferredLocation['spc_geo'] = geoFromRole
                inferredLocation['spc_geo_metadata'] = {
                    'source': 'role',
                    'source_id': int(row['role_id']),
                }
                break
    return pd.Series(inferredLocation)


if __name__ == '__main__':
    print('Starting script!')
    # Create the connection
    conn = cnx.Cnx

    tqdm.pandas()
    print('Getting role data...')
    roleData = pd.read_sql(roleQuery, conn)
    grouped = roleData.groupby(['person_id', 'full_name'], as_index=False)
    print('Running geo inference...')
    inferredDf = grouped.progress_apply(inferPersonGeo)
    locationsToWrite = inferredDf[locationDbCols]
    print('Writing to database...')
    locationsToWrite.to_sql(
        'person_locations',
        conn,
        if_exists='replace',
        index=False,
        dtype={
            'location_metadata': sqlalchemy.dialects.postgresql.JSONB,
            'spc_geo_metadata': sqlalchemy.dialects.postgresql.JSONB,
        },
        schema='score_v2',
    )
    print('Done!')
