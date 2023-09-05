#!/bin/bash
echo '[CONSOLE] activating sandbox.'
eval "$(conda shell.bash hook)"
conda activate spc-sandbox
echo '[CONSOLE] executing traffic_flags_isr.py'
python traffic_flags/traffic_flags_isr.py
echo '[CONSOLE] executing company_sweetspot_flags_isr.py'
python company_sweetspot_flags/company_sweetspot_flags_isr.py
echo '[CONSOLE] executing role_score_isr.py'
python role_score/role_score_isr.py
echo '[CONSOLE] executing education_score_isr.py'
python education_score/education_score_isr.py
echo '[CONSOLE] executing person_score_isr.py'
python person_score/person_score_isr.py
echo '[CONSOLE] executing hs_score_isr.py'
python haystack_score/hs_score_isr.py
echo '[CONSOLE] executing hs_uploads_isr.py'
python haystack_score/hs_uploads_isr.py
echo '[CONSOLE] executing stealth_isr.py'
python stealth_founders/stealth_isr.py