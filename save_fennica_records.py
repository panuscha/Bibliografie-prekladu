import pandas as pd
from pymarc import map_xml, MARCWriter

path = 'data/fennica.mrcx'

OUT = 'data/fennica_preklady.mrc'

df_path = 'data/preklady/Bibliografie_prekladu_fin.csv'
df = pd.read_csv(df_path, encoding='utf_8')

fennica_id = df['Finské id']
fennica_id = [str(id) for id in fennica_id] 
fennica_id = set(fennica_id)
fennica_id.remove('nan')
#print(fennica_id)

writer = MARCWriter(open(OUT, "wb"))

def do_it(r):
    global writer, fennica_id
    for field in r.get_fields('001'):
        if str(field.data) in fennica_id:
            writer.write(r)
            #print(r)
            fennica_id.remove(field.data)


try:
    map_xml(do_it, path)
    print("Not found: ", fennica_id)
except Exception as error:
    print("Exception: " + type(error).__name__)      
finally:
    writer.close() 