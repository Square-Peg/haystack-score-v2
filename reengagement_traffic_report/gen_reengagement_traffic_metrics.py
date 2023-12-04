import os

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import sqlalchemy
from dotenv import load_dotenv
from scipy.optimize import curve_fit
from scipy.stats import linregress
from tqdm import tqdm

# Load environment variables
load_dotenv()
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DBNAME = os.getenv("POSTGRES_DBNAME")
HS_SCORE_V2_DIR = os.getenv("HS_SCORE_V2_DIR")

# Connection engine
postgresStr = "postgresql://{username}:{password}@{host}:{port}/{dbname}".format(
    username=POSTGRES_USER,
    password=POSTGRES_PASSWORD,
    host=POSTGRES_HOST,
    port=POSTGRES_PORT,
    dbname=POSTGRES_DBNAME,
)
# Create the connection
cnx = sqlalchemy.create_engine(postgresStr)

# Pull data from traffic
traffic_data_query = """
select t.*
from similarweb_traffic_monthly t
where t.domain in (
    select website as domain
    from crm_exports crme 
    where crme.affinity_list = 'global_pipeline'
    and crme.status <> 'No'
)
"""

if __name__ == "__main__":
    print("Pulling traffic data...")

    raw_traffic = pd.read_sql(traffic_data_query, cnx)

    print("Cleaning traffic data...")
    # Select only domains that have had more than 10k visits in the last 3 months
    latest_date = raw_traffic["visit_date"].max()
    cutoff = latest_date - pd.DateOffset(months=3)
    last3months = raw_traffic[raw_traffic["visit_date"] >= cutoff]
    last3months = last3months.groupby("domain").sum("visit_count")
    filter_list = last3months[last3months["visit_count"] >= 3000].index.tolist()
    traffic = raw_traffic[raw_traffic["domain"].isin(filter_list)]

    # Sort the DataFrame by domain and visit_date
    df_sorted = traffic.sort_values(["domain", "visit_date"])

    # Group the DataFrame by domain
    grouped = df_sorted.groupby("domain")
    print("Domains to calculate metrics for: {}".format(len(grouped)))

    # Function representing the exponential curve
    print("Calculating exponential and linear curve fits for each domain...")

    def exponential_curve(x, a, b, c):
        return a * np.exp(-b * x) + c

    ### Calculate linear and exponential curve fits for each domain
    results_exp = {}
    results_lin = {}

    for domain, group in tqdm(grouped):
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

            # Store the results in the dictionary
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

    for domain, group in tqdm(grouped):
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

    print("Done calculating curve fits.")

    print("Plotting charts...")

    # For every domain, generate and save traffic chart with both exponential and linear curves
    for domain, group in tqdm(grouped):
        # Get the coefficients
        a, b, c = results_df.loc[
            domain, ["coefficient_a", "coefficient_b", "coefficient_c"]
        ]

        # Generate the curves
        x = np.arange(len(group))
        y_lin = (
            results_df.loc[domain, "slope"] * x + results_df.loc[domain, "intercept"]
        )
        y_exp = exponential_curve(x, a, b, c)

        # Plot the data and the curves as dashed lines
        sns.set_style("whitegrid")

        sns.lineplot(x=x, y=y_exp, color="red", alpha=0.5)
        sns.lineplot(x=x, y=y_lin, color="purple", alpha=0.5)

        sns.lineplot(x=x, y=group["visit_count"].values, color="blue")

        # Add a title
        plt.title(domain)

        # Set x ticks to be the first day of every month
        plt.xticks(
            x, group["visit_date"].dt.strftime("%b '%y"), rotation=60, fontsize=8
        )

        # Add x and y labels
        plt.xlabel("Date")
        plt.ylabel("Visit Count")

        # Save the plot
        plt.savefig(
            HS_SCORE_V2_DIR
            + "_reengagement_traffic_report/charts/{}.png".format(domain)
        )
        plt.clf()
        plt.close()

    print("Done plotting charts.")

    print("Making growth flags...")
    # Calculate growth flags
    grouped_df = (
        traffic.groupby("domain")
        .agg(
            last_24_months_total=("visit_count", "sum"),
            last_24_months_mean=("visit_count", "mean"),
            last_3_months_mean=("visit_count", lambda x: x.tail(3).mean()),
            mom_perc_growth=(
                "visit_count",
                lambda x: (x.iloc[-1] / x.iloc[-2] - 1) * 100,
            ),
            qoq_perc_growth=(
                "visit_count",
                lambda x: (x.iloc[-1] / x.iloc[-4] - 1) * 100,
            ),
            yoy_perc_growth=(
                "visit_count",
                lambda x: (x.iloc[-1] / x.iloc[-13] - 1) * 100,
            ),
        )
        .reset_index()
    )

    # Combine metrics with the results_df
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
    # last_3_months_mean_bucket = pd.cut(all_calcs['last_3_months_mean'], bins=[0, b1, b2, b3, np.inf], labels=['na', 'low', 'med', 'high'])
    last_3_months_mean_bucket = pd.cut(
        all_calcs["last_3_months_mean"],
        bins=[0, 100, 200, 500, np.inf],
        labels=["na", "low", "med", "high"],
    )

    # add bucketed column to dataframe
    all_calcs["last_3_months_mean_bucket"] = last_3_months_mean_bucket

    print("Adding metadata from Affinity...")
    # Pull affinity_organisation_id from db for metadata
    domain_list = all_calcs["domain"].tolist()

    with_affinity = pd.read_sql_query(
        """                              
        SELECT website as domain, crme.affinity_organisation_id, geo.spc_geo, crme.time_in_current_status, crme.status
        FROM crm_exports crme
        left join crm_geo geo on geo.affinity_organisation_id = crme.affinity_organisation_id
        WHERE website IN %(domains)s
    """,
        con=cnx,
        params={"domains": tuple(domain_list)},
    )

    with_geo = pd.merge(all_calcs, with_affinity, on="domain")

    # dedupe
    with_geo = with_geo.drop_duplicates(subset=["domain"])

    print("Creating priority flags...")
    # filter for upward trending domains
    upward_trend = with_geo[with_geo["lin_curve"] == True]

    # generate rank per bucket
    upward_trend["rank"] = upward_trend.groupby(
        ["last_3_months_mean_bucket", "spc_geo"]
    )["r_squared"].rank(ascending=False)

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

    print("Writing to csv...")
    # write to csv
    current_date = pd.to_datetime("today").strftime("%Y%m%d")
    final_df.to_csv(
        HS_SCORE_V2_DIR
        + "_reengagement_traffic_report/reengagement_metrics_{}.csv".format(
            current_date
        ),
        index=False,
    )

    print("Done!")
