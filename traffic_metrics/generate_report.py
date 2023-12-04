import math
from jinja2 import Environment, FileSystemLoader
from datetime import datetime
import pandas as pd
from context import cnx
from dotenv import load_dotenv
import os

load_dotenv()
HS_SCORE_V2_DIR = os.getenv("HS_SCORE_V2_DIR")
MONTHLY_TRAFFIC_TEMPLATE_FILEPATH = (
    HS_SCORE_V2_DIR + "traffic_metrics/monthly_report/report_template.html"
)
MONTHLY_TRAFFIC_REPORT_FILEPATH = HS_SCORE_V2_DIR + "monthly_report/report.html"


# Define the formatNumber filter
def format_number(value):
    if math.isinf(value) or math.isnan(value):
        return str(value)
    return "{:,.0f}".format(value)


# Load the HTML template
env = Environment(loader=FileSystemLoader("/"))
env.filters["formatNumber"] = format_number
template = env.get_template(MONTHLY_TRAFFIC_TEMPLATE_FILEPATH)

# Read traffic metrics from DB
conn = cnx.Cnx
traffic_metrics = pd.read_sql(
    """
select * from score_v2.similarweb_traffic_metrics
    """,
    conn,
)

# Sort group by 'lin_curve' and 'exp_curve' columns
traffic_metrics = traffic_metrics.sort_values(
    by=["exp_curve", "lin_curve"], ascending=False
)

date = datetime.now().strftime("%d %b %Y")

# Render the template with the data
rendered_html = template.render(date=date, data=traffic_metrics)

# Save the rendered HTML to a file
with open(MONTHLY_TRAFFIC_REPORT_FILEPATH, "w") as file:
    file.write(rendered_html)
