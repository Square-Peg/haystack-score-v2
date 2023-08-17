#!/bin/bash
echo '[CONSOLE] activating sandbox.'
eval "$(conda shell.bash hook)"
conda activate spc-sandbox
echo '[CONSOLE] executing traffic_flags_anz.py'
python traffic_flags/traffic_flags_anz.py
echo '[CONSOLE] executing company_sweetspot_flags_anz.py'
python company_sweetspot_flags/company_sweetspot_flags_anz.py
echo '[CONSOLE] executing role_score_anz.py'
python role_score/role_score_anz.py
echo '[CONSOLE] executing education_score_anz.py'
python education_score/education_score_anz.py
echo '[CONSOLE] executing person_score_anz.py'
python person_score/person_score_anz.py
echo '[CONSOLE] executing hs_score_anz.py'
python haystack_score/hs_score_anz.py
echo '[CONSOLE] executing hs_uploads_anz.py'
python haystack_score/hs_uploads_anz.py
echo '[CONSOLE] executing stealth_anz.py'
python stealth_founders/stealth_anz.py