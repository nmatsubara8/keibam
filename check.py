import pandas as pd
df = pd.read_pickle('data/raw/results.pkl')
print('shape:', df.shape)
print('race_id col dtype:', df['race_id'].dtype if 'race_id' in df.columns else 'NO col')
print('race_id sample:', df['race_id'].head(3).tolist() if 'race_id' in df.columns else 'N/A')
