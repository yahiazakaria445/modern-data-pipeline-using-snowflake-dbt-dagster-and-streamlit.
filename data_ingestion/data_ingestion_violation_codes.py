import pandas as pd
import snowflake.connector
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
    role=config["role"]
)
cur = conn.cursor()


csv_file_path = "E:/AI 2/DE/dbt/Airbyte_Snowflake_Dbt_Greatexpectations_Streamlit/data/dof_parking_violation_codes.csv"
df = pd.read_csv(csv_file_path)

insert_query = """
INSERT INTO RAW.parking_violation_codes (CODE, DEFINITION, Manhattan_96th_St_below, All_Other_Areas)
VALUES (%s, %s, %s, %s)
"""

data = df.where(pd.notnull(df), None).values.tolist()
cur.executemany(insert_query, data)
conn.commit()

print("Data uploaded successfully")
cur.close()
conn.close()