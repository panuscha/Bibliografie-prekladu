import pandas as pd
import numpy as np

print("Reading excel file")
df = pd.read_excel("data/preklady/excel/Bibliografie překladů - tabulka (s příklady) - řečtina.xlsx", index_col=0)
df = df.reset_index()  

print(len(df))


print("Converting to CSV")
y_nan = pd.isnull(df.loc[:, df.columns != 'Číslo záznamu']).all(1).to_numpy().nonzero()[0]
y2_nan = pd.isnull(df.loc[:, :]).all(1).to_numpy().nonzero()[0]   
y_nan = np.append(y_nan, y2_nan)
y_nan = np.append([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 ], y_nan)



df = df.drop(df.index[y_nan]).copy(deep=True)

for column in df.columns:
    df[column] = df[column].map(lambda x: str(x).strip())
    
print(len(df))

    
df.to_csv('data/preklady/Bibliografie_prekladu_gre.csv',  encoding='utf-8')