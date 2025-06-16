import pymarc
from pymarc import Record, MARCReader, Subfield, Reader, Field
import pandas as pd
import pickle
import numpy as np
from collections import defaultdict
from ordered_set import OrderedSet
from itertools import zip_longest
# trl0012787

def change_500_to_561(record): 
    for field in record.get_fields('500'):
        if field['a'] in ['Marie Rákosníková: Recepce české literatury v Řecku. Bibliografie české literatury vydané v novořečtině. Diplomová práce. FF UK, Praha 2022.', 'Hana Hlinovská: Bibliografie překladů české literatury do finštiny. Bakalářská práce. FF MU, Brno 2024.'] :
            delete = field
    record.remove_field(delete)
    delete.tag = '561'
    record.add_ordered_field(delete)
    return record

if __name__ == "__main__":
    OUT = 'data/marc_bibliografie_prekladu_combined.mrc'
    used_001 = []
    n_together = 0 # number of records together
    with open(OUT , 'wb') as writer:
        # for l in ['fin','gre', 'ita']: 
        #     n_basis = 0 # number of records in the basis 
        #     with open(f'data/marc_duplicities_{l}_opraveno.mrc', 'rb') as data:
        #         reader = MARCReader(data, to_unicode=True, force_utf8=True, utf8_handling="strict")
        #         for record in reader: 
        #             record.remove_fields('915')
        #             record.add_ordered_field(Field(tag = '915', indicators=[' ', ' '], subfields=[Subfield(code = 'a', value = True)]))
        #             writer.write(record.as_marc() if l == 'ita' else change_500_to_561(record).as_marc()) 
        #             used_001.append(record['001'].data)
        #             n_basis += 1
        #     print(f'Basis {l}: {n_basis}')   
        #     n_together += n_basis      
        with open(f'data/marc_bibliografie_prekladu_a_d_add_trl_3.mrc'  , 'rb') as data:
            reader = MARCReader(data, to_unicode=True, force_utf8=True, utf8_handling="strict")
            for record in reader:  
                if record['001'].data not in used_001:
                    writer.write(record.as_marc()) 
                    n_together += 1
       

        with open(f'data/ucla_trl.mrc', 'rb') as data:
            reader = MARCReader(data, to_unicode=True, force_utf8=True, utf8_handling="strict")
            for record in reader:  
                if record['001'].data not in used_001:
                    writer.write(record.as_marc()) 
                    n_together += 1
        print(f'Together: {n_together}')            

