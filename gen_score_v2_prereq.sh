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
echo '[CONSOLE] executing get_sw_url_list.py'
python sw_url_list/get_sw_url_list.py