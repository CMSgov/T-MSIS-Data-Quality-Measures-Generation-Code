import pandas as pd

df = pd.read_csv("./dqm/cfg/thresholds.csv")

print(df.head())
df.to_pickle('./dqm/cfg/thresholds.pkl')
