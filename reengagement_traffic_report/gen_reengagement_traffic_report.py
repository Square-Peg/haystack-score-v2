import math
import os
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv
from jinja2 import Environment, FileSystemLoader

load_dotenv()
HS_SCORE_V2_DIR = os.getenv("HS_SCORE_V2_DIR")


# Define the formatNumber filter
def format_number(value):
    if math.isinf(value) or math.isnan(value):
        return str(value)
    return "{:,.0f}".format(value)


# Load the HTML template
env = Environment(loader=FileSystemLoader(HS_SCORE_V2_DIR))
env.filters["formatNumber"] = format_number
template = env.get_template("/reengagement_traffic_report/report_template.html")

# Read the traffic metrics CSV file
current_date = datetime.now().strftime("%Y%m%d")
traffic_metrics = pd.read_csv(
    HS_SCORE_V2_DIR
    + "_reengagement_traffic_report/metrics/reengagement_metrics_{}.csv".format(
        current_date
    )
)

# Sort group by 'lin_curve' and 'exp_curve' columns
traffic_metrics = traffic_metrics.sort_values(
    by=["exp_curve", "lin_curve"], ascending=False
)

date = datetime.now().strftime("%d %b %Y")

# Render the template with the data
rendered_html = template.render(date=date, data=traffic_metrics)

# Save the rendered HTML to a file
with open(
    HS_SCORE_V2_DIR
    + "_reengagement_traffic_report/reports/reengagement_report_{}.html".format(
        current_date
    ),
    "w",
) as file:
    file.write(rendered_html)
