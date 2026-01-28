import pandas as pd
import sys

print("arguments: {0}".format(sys.argv))

month = int(sys.argv[1])


print ("Running pipeline for month {0}".format(month))

df = pd.DataFrame({"A": [1, 2], "B":[3, 4]})
print(df.head())

df.to_parquet("output_month_{0}.parquet".format(month))

print("Pipeline finished successfully!")


