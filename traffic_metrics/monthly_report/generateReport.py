import math
from jinja2 import Environment, FileSystemLoader
from datetime import datetime
import pandas as pd


# Define the formatNumber filter
def format_number(value):
    if math.isinf(value) or math.isnan(value):
        return str(value)
    return "{:,.0f}".format(value)


# Load the HTML template
env = Environment(loader=FileSystemLoader('.'))
env.filters['formatNumber'] = format_number
template = env.get_template('report_template.html')

# Read the traffic metrics CSV file
traffic_metrics = pd.read_csv('./data/traffic_metrics.csv')

# Sort group by 'lin_curve' and 'exp_curve' columns
traffic_metrics = traffic_metrics.sort_values(
    by=['exp_curve', 'lin_curve'], ascending=False
)

date = datetime.now().strftime("%d %b %Y")

# Render the template with the data
rendered_html = template.render(date=date, data=traffic_metrics)

# Save the rendered HTML to a file
with open('report.html', 'w') as file:
    file.write(rendered_html)
