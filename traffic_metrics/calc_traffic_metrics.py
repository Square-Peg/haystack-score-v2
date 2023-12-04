import datetime

import numpy as np
import pandas as pd
from context import cnx
from scipy.optimize import curve_fit
from scipy.stats import linregress
from dotenv import load_dotenv
import os

load_dotenv()
HS_SCORE_V2_DIR = os.getenv("HS_SCORE_V2_DIR")
print("Starting script...")

CURRENT_TS = pd.to_datetime("today").strftime("%Y%m%d_%H_%M_%S")

# Create the connection
conn = cnx.Cnx

# Pull data from traffic
raw_traffic = pd.read_sql(
    """
select t.*
from similarweb_traffic_monthly t
""",
    conn,
)

# Sort the DataFrame by domain and visit_date
df_sorted = raw_traffic.sort_values(["domain", "visit_date"])

# Group the DataFrame by domain
grouped = df_sorted.groupby("domain")


## Calc exponential fit
def exponential_curve(x, a, b, c):
    return a * np.exp(-b * x) + c


results_exp = {}

for domain, group in grouped:
    x = np.arange(len(group))  # x-values for the curve fit
    y = group["visit_count"].values  # y-values for the curve fit

    try:
        # Perform curve fitting
        popt, pcov = curve_fit(exponential_curve, x, y)

        # Get the coefficients and covariance matrix
        a, b, c = popt
        covariance_matrix = pcov

        # Calculate the goodness of fit (R-squared)
        residuals = y - exponential_curve(x, a, b, c)
        ss_residuals = np.sum(residuals**2)
        ss_total = np.sum((y - np.mean(y)) ** 2)
        r_squared = 1 - (ss_residuals / ss_total)

        results_exp[domain] = {
            "coefficient_a": a,
            "coefficient_b": b,
            "coefficient_c": c,
            "covariance_matrix": covariance_matrix,
            "r_squared": r_squared,
        }
    except RuntimeError:
        # Set coefficients and fit result to 0 for domains with RuntimeError
        results_exp[domain] = {
            "coefficient_a": 0,
            "coefficient_b": 0,
            "coefficient_c": 0,
            "covariance_matrix": np.zeros((3, 3)),
            "r_squared": 0,
        }


## Calc linear fit
results_lin = {}

for domain, group in grouped:
    x = np.arange(len(group))  # x-values for the linear regression
    y = group["visit_count"].values  # y-values for the linear regression

    try:
        # Perform linear regression
        slope, intercept, r_value, p_value, std_err = linregress(x, y)

        # Store the results in the dictionary
        results_lin[domain] = {
            "slope": slope,
            "intercept": intercept,
            "r_value": r_value,
            "p_value": p_value,
            "std_err": std_err,
        }
    except Exception:
        # Set coefficients and fit result to 0 for domains with errors
        results_lin[domain] = {
            "slope": 0,
            "intercept": 0,
            "r_value": 0,
            "p_value": 0,
            "std_err": 0,
        }

# make a dataframe combinding both results dictionaries
results_df_exp = pd.DataFrame.from_dict(results_exp, orient="index")
results_df_lin = pd.DataFrame.from_dict(results_lin, orient="index")
results_df = pd.concat([results_df_exp, results_df_lin], axis=1)
results_df.columns = [
    "coefficient_a",
    "coefficient_b",
    "coefficient_c",
    "covariance_matrix",
    "r_squared",
    "slope",
    "intercept",
    "r_value",
    "p_value",
    "std_err",
]

# Create 'exp_curve' column, TRUE (if goodness > 0.3 AND if it is a growing curve), FALSE otherwise
results_df["exp_curve"] = (results_df["r_squared"] > 0.3) & (
    results_df["coefficient_a"] > 1
)

# Create 'lin_curve' column, TRUE (if p_value < 0.05 AND if trend is positive), FALSE otherwise
results_df["lin_curve"] = (results_df["p_value"] < 0.05) & (results_df["slope"] > 0)

# Group by 'domain' and calculate the required metrics
grouped_df = (
    raw_traffic.groupby("domain")
    .agg(
        last_24_months_total=("visit_count", "sum"),
        last_24_months_mean=("visit_count", "mean"),
        last_3_months_mean=("visit_count", lambda x: x.tail(3).mean()),
        mom_perc_growth=("visit_count", lambda x: (x.iloc[-1] / x.iloc[-2] - 1) * 100),
        qoq_perc_growth=("visit_count", lambda x: (x.iloc[-1] / x.iloc[-4] - 1) * 100),
        yoy_perc_growth=("visit_count", lambda x: (x.iloc[-1] / x.iloc[-13] - 1) * 100),
    )
    .reset_index()
)

# Combine fit metrics with the results_df
all_calcs = pd.merge(
    grouped_df.copy(),
    results_df[["r_squared", "exp_curve", "p_value", "lin_curve"]],
    left_on="domain",
    right_index=True,
)

# calculate last_3_months_mean quartiles
b1 = all_calcs["last_3_months_mean"].quantile(0.5)
b2 = all_calcs["last_3_months_mean"].quantile(0.7)
b3 = all_calcs["last_3_months_mean"].quantile(0.9)

# create bucketed column
last_3_months_mean_bucket = pd.cut(
    all_calcs["last_3_months_mean"],
    bins=[0, b1, b2, b3, np.inf],
    labels=["na", "low", "med", "high"],
)

# add bucketed column to dataframe
all_calcs["last_3_months_mean_bucket"] = last_3_months_mean_bucket

# Pull affinity_organisation_id from db
domain_list = all_calcs["domain"].tolist()

with_affinity = pd.read_sql_query(
    """                              
    SELECT website as domain, crme.affinity_organisation_id, geo.spc_geo, crme.time_in_current_status, crme.status, crme.name
    FROM crm_exports crme
    left join crm_geo geo on geo.affinity_organisation_id = crme.affinity_organisation_id
    WHERE website IN %(domains)s
""",
    con=conn,
    params={"domains": tuple(domain_list)},
)


with_geo = pd.merge(all_calcs, with_affinity, on="domain")

# dedupe
with_geo = with_geo.drop_duplicates(subset=["domain"])

# filter for upward trending domains
upward_trend = with_geo[with_geo["lin_curve"] == True]
upward_trend = upward_trend[upward_trend["r_squared"] > 0.3]


# filter for only non-no's
upward_trend = upward_trend[upward_trend["status"] != "No"]

# generate rank per bucket
upward_trend["rank"] = upward_trend.groupby(["last_3_months_mean_bucket", "spc_geo"])[
    "r_squared"
].rank(ascending=False)

# flag high priority domains (rank 1-10)
upward_trend["high_priority"] = upward_trend["rank"].apply(
    lambda x: True if x <= 10 else False
)

# merge back to final_df
final_df = pd.merge(
    with_geo, upward_trend[["domain", "high_priority"]], on="domain", how="left"
)

final_df["high_priority"] = final_df["high_priority"].fillna(False)

# Set all numbers to 2 decimal places
final_df = final_df.round(2)


# Clean before writing to db
final_df["generated_at"] = datetime.datetime.now()
final_df = final_df.replace([np.inf, -np.inf], np.nan)

# write to db
write_res = final_df.to_sql(
    "similarweb_traffic_metrics",
    conn,
    if_exists="replace",
    index=False,
    schema="score_v2",
)

# write high priority to file
high_priority = final_df[final_df["high_priority"] == True]
high_priority = high_priority[high_priority["spc_geo"] == "SEA"]
high_priority = high_priority[high_priority["last_3_months_mean_bucket"] != "na"]
high_priority = high_priority[
    [
        "affinity_organisation_id",
        "domain",
        "name",
        "r_squared",
        "last_3_months_mean_bucket",
        "generated_at",
    ]
]


def gen_note_string(row):
    note_string = """Domain identified as high priority based on web traffic.
Last 3 months traffic bucket: {traffic_bucket}
R-squared: {r_squared}
Generated at: {generated_at}
    """.format(
        traffic_bucket=row["last_3_months_mean_bucket"],
        r_squared=row["r_squared"],
        generated_at=row["generated_at"],
    )
    return note_string


high_priority["notes"] = high_priority.apply(gen_note_string, axis=1)

high_priority_final = high_priority[
    ["affinity_organisation_id", "domain", "name", "notes"]
].copy()

high_priority_final.columns = ["affinity_organisation_id", "Website", "Name", "Notes"]

high_priority_final["Status"] = "Review"


high_priority_final.to_csv(
    HS_SCORE_V2_DIR
    + "_traffic_prio_upload/traffic_prio_upload_{}.csv".format(CURRENT_TS),
    index=False,
)

print("Finished!")
