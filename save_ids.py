from pymarc import Record, MARCReader, Subfield
import pandas as pd
from pymarc.field import Field
import pickle 

marc_it_file = "data/marc_it.mrc"

dict_a_w = open("data/dict_author_work.obj",'rb')
dict_author_work = pickle.load(dict_a_w)

with open(marc_it_file, 'rb') as data:
    reader = MARCReader(data, to_unicode=True, force_utf8=True, utf8_handling="strict")
    for record in reader:
        for field in  record.get_fields('595') :
            if '$a' in str(field) and  '$t' in str(field) and  '$1' in str(field):
                author = field['a'].lower() 
                title = field['t'].lower()
                id = field['1'] 
                if author in dict_author_work.keys() and title not in dict_author_work[author].keys():
                    dict_titles = dict_author_work[author]
                    dict_titles[title] = id
                    dict_author_work[author] = dict_titles

with open('dict_author_work', 'wb') as f: 
    pickle.dump(dict_author_work, f)                      
                    
                    
