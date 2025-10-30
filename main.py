from time import time
import pandas as pd
from queries import complianceQuery
from sitemapping import site_mapping  
from credentials import USER, PASSWORD, ACCOUNT, WAREHOUSE, DATABASE, SCHEMA
import snowflake.connector

#-- Start timer
start = time.time()

print("Starting Snowflake connection...")

# -- Connect to Snowflake, credentials imported from credentials.py
conn = snowflake.connector.connect(
    user=USER,
    password=PASSWORD,
    account=ACCOUNT,
    warehouse=WAREHOUSE,
    database=DATABASE,
    schema=SCHEMA
)


print("Connection to Snowflake established.")

#-- Parameters
timeDays = 7
excel_file = "all_sites_compliance.xlsx"
all_data = {}

# -- Run the query and get results as a DataFrame
print("Running query...")

#-- Runs query for each site and stores results in all_data dict
for siteFull, siteShort in site_mapping.items():

    print(f"Running query for {siteShort} ({siteFull})...")
    cur = conn.cursor()
    try:

        sql = complianceQuery(siteShort, siteFull, timeDays)
        cur.execute(sql)

        df = cur.fetch_pandas_all()
        all_data[siteShort] = df

        print(f" Done: {siteShort} ({len(df)} rows)")
    except Exception as e:

        print(f" Error for {siteShort}: {e}")
    finally:

        cur.close()

conn.close()

print("All queries done. Saving to Excel...")

#-- Save all data to a single Excel file with multiple sheets
with pd.ExcelWriter(excel_file, engine="openpyxl") as writer:

    for siteShort, df in all_data.items():

        sheet_name = siteShort[:31]
        df.to_excel(writer, sheet_name=sheet_name, index=False)

print(f" All site data saved to {excel_file}")

#-- End timer
end = time.time()
elapsed = end - start
print(f"Script completed in {elapsed:.2f} seconds.")
