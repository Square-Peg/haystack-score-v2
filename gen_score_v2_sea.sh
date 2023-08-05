#!/bin/bash
echo '[CONSOLE] activating sandbox.'
eval "$(conda shell.bash hook)"
conda activate spc-sandbox
echo '[CONSOLE] executing person_locations.py'
python infer_locations/person_locations.py
echo '[CONSOLE] executing company_locations.py'
python infer_locations/company_locations.py
echo '[CONSOLE] executing education_flags.py'
python flags/education_flags.py
echo '[CONSOLE] executing role_flags.py'
python flags/role_flags.py
echo '[CONSOLE] executing person_flags.py'
python flags/person_flags.py
echo '[CONSOLE] executing company_flags.py'
python flags/company_flags.py
echo '[CONSOLE] executing traffic_flags_sea.py'
python traffic_flags/traffic_flags_sea.py
echo '[CONSOLE] executing company_sweetspot_flags_sea.py'
python company_sweetspot_flags/company_sweetspot_flags_sea.py
echo '[CONSOLE] executing role_score_sea.py'
python role_score/role_score_sea.py
echo '[CONSOLE] executing education_score_sea.py'
python education_score/education_score_sea.py
echo '[CONSOLE] executing person_score_sea.py'
python person_score/person_score_sea.py
echo '[CONSOLE] executing hs_score_sea.py'
python haystack_score/hs_score_sea.py
echo '[CONSOLE] executing hs_uploads_sea.py'
python haystack_score/hs_uploads_sea.py
