import sys
import pandas as pd
print("arguments", sys.argv)

month = int(sys.argv[1])
print(f"Running pipeline for month {month}")

df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
df['month'] = month
print(df.head())

df.to_parquet(f"output_month_{sys.argv[1]}.parquet")