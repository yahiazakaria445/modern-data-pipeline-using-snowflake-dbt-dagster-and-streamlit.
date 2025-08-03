import snowflake.connector
import pandas as pd
from tqdm import tqdm
import json


with open("sf_config.json", "r") as f:
    config = json.load(f)


conn = snowflake.connector.connect(
    user=config["user"],
    password=config["password"],
    account=config["account"],
    warehouse=config["warehouse"],
    database=config["database"],
    schema=config["schema"],
    role=config["role"],
    client_session_keep_alive=True
)
cur = conn.cursor()


file_path = "E:/AI 2/DE/dbt/Airbyte_Snowflake_Dbt_Greatexpectations_Streamlit/data/parking_violations_issued_fiscal_year_2023_sample.csv"
df = pd.read_csv(file_path)


insert_sql = """
INSERT INTO RAW.parking_violations_issued_fiscal_year_2023_sample (
    summons_number, registration_state, plate_type, issue_date, violation_code,
    vehicle_body_type, vehicle_make, issuing_agency, vehicle_expiration_date,
    violation_location, violation_precinct, issuer_precinct, issuer_code,
    issuer_command, issuer_squad, violation_time, time_first_observed,
    violation_county, violation_in_front_of_or_opposite, date_first_observed,
    law_section, sub_division, violation_legal_code, days_parking_in_effect,
    from_hours_in_effect, to_hours_in_effect, vehicle_color,
    unregistered_vehicle, vehicle_year, meter_number, feet_from_curb,
    no_standing_or_stopping_violation, hydrant_violation,
    double_parking_violation
) VALUES ({});
"""


for _, row in tqdm(df.iterrows(), total=len(df), desc="Uploading to Snowflake"):
    values = ', '.join(
        ["NULL" if pd.isna(v) else f"'{str(v).replace('\'', '\'\'')}'" for v in row]
    )
    cur.execute(insert_sql.format(values))

cur.close()
conn.close()
print("Upload completed successfully")
