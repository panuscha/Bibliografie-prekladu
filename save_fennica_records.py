import pandas as pd
from pymarc import map_xml, MARCWriter

path = 'D:/Panuskova/Nextcloud/Bibliografie prekladu/data/fennica.mrcx'

OUT = 'D:/Panuskova/Nextcloud/Bibliografie prekladu/data/fennica_preklady.mrc'

df_path = 'D:/Panuskova/Nextcloud/Bibliografie prekladu/data/preklady/Bibliografie_prekladu_fin.csv'
df = pd.read_csv(df_path, encoding='utf_8')

fennica_id = df['Finské id']
fennica_id = [str(id) for id in fennica_id] 
print(fennica_id)

writer = MARCWriter(open("D:/Panuskova/Nextcloud/Bibliografie prekladu/data/fennica_preklady", "wb"))

def do_it(r):
    global writer
    for field in r.get_fields('001'):
        if str(field.data) in fennica_id:
            writer.write(r)
            print(r)
            
try:
    map_xml(do_it, path)
except Exception as error:
    print("Exception: " + type(error).__name__)      
finally:
    writer.close() 