import pandas as pd
from pymarc import map_xml
path = 'data/fennica.mrcx'

def do_it(r):
    for field in r.get_fields('041'):
        if '$a' in str(field) and field['a'] == 'cze':
            print(r)
        if '$h' in str(field) and field['h'] == 'cze':
            print(r)    
            

map_xml(do_it, path)