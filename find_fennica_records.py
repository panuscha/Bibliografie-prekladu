import pandas as pd
from pymarc import map_xml

path = 'data/fennica.mrcx'

OUT = 'data/fennica_preklady.mrc'

df_path = 'data/preklady/Bibliografie_prekladu_fin.csv'
df = pd.read_excel(df_path)

fennica_id = df['Finské id']

def do_it(r):
    for field in r.get_fields('001'):
        if '$a' in str(field) and field['a'] == 'cze':
            print(r)
        if '$h' in str(field) and field['h'] == 'cze':
            print(r)    
            
with open(OUT , 'wb') as writer:
    map_xml(do_it, path)