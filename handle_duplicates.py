import pymarc
from pymarc import Record, MARCReader, Subfield, Reader, Field
import pandas as pd
import pickle
import numpy as np
from collections import defaultdict
from ordered_set import OrderedSet
from itertools import zip_longest


def select_languages_from_trl(lang, path):
    ret = defaultdict(list)
    with open(path, 'rb') as data:
        reader = MARCReader(data, to_unicode=True, force_utf8=True, utf8_handling="strict")
        for record in reader:
            remember_record = False
            if record is None: print('Record id None')
            else:
                for field in record.get_fields('041'):
                    for language in field.get_subfields('a'):
                        if language == lang: 
                            remember_record = True
                            break
                if remember_record:
                    for field in record.get_fields('100'):
                        subfields = field.subfields_as_dict()
                        if 'a' in subfields.keys(): ret[subfields['a'][0]].append(record) 
                        if '7' in subfields.keys(): ret[subfields['7'][0]].append(record)    
    return ret

def check_year(record_trl, record):
    year = None
    year_trl = None
    for field in record.get_fields('264'):
        subfield = field.subfields_as_dict()
        year = field['c'].strip(' []') if 'c' in subfield.keys() else  None

    for i in ['260', '264']:
        for field in record_trl.get_fields(i):
            subfield = field.subfields_as_dict()
            if 'c' in subfield.keys():
                year_trl = field['c'].strip(' []') 
                break

    return  year is not None and year_trl is not None and year == year_trl
             


def check_595(record_trl, record ) :
    record_trl_code = '1'
    record_code = '2'
    for field in record_trl.get_fields('595'):
        subfields = field.subfields_as_dict()
        if '1' in subfields.keys(): record_trl_code = subfields['1'][0]  
    for field in record.get_fields('595'):
        subfields = field.subfields_as_dict()
        if '1' in subfields.keys(): record_code = subfields['1'][0].replace('-', '') 
    return  record_code == record_trl_code

def write_marc_dupl( writer, record, record_trl, schon_besetzt):
    id_001 = record['001'].data
    if id_001 in schon_besetzt.keys():
        trl_eintrag = schon_besetzt[id_001][1]
        for feld in trl_eintrag.get_fields('900'):
            alt_900 = feld['a']
        for feld in record_trl.get_fields('900'):
            neu_900 = feld['a'] 
        if alt_900 == 'ABA001' and neu_900 != 'ABA001':
            return schon_besetzt
        if alt_900 == 'ABA001' and neu_900 == 'ABA001': 
            neues_id = trl_eintrag['001']
            altes_id = record_trl['001']
            print(f'BEIDE ABA001 id: {id_001} \n trl ids:{neues_id} {altes_id}') 
            return schon_besetzt   
               
    author = record['100']['a']
    schon_besetzt[id_001] = [record, record_trl]
    print(f'{record.title} {author}')
    writer.write(record.as_marc())
    writer.write(record_trl.as_marc())
    return schon_besetzt

def combine_008(r_008, trl_008):
    new_008 = ''
    for i,j in zip( r_008, trl_008):
        new_008+= i if j in ['\\', '|'] else j
    print(new_008)  
    return new_008  

def subset_keys(feld1_sub, feld2_sub):
    return all(a in list(feld2_sub.keys()) for a in list(feld1_sub.keys())  )

def decide_field(record, record_trl, rest):
    print(record)
    print(record_trl)
    for tag in rest:
        for feld1, feld2 in zip_longest(record.get_fields('264' if tag == '260' else tag), record_trl.get_fields(tag)):
            print(f'NEW: {feld1}')
            print(f'OLD: {feld2}')
            if feld1 is None:
                record.add_ordered_field(feld2)
            elif feld2 is not None:    
                feld1_sub =  feld1.subfields_as_dict()
                feld2_sub = feld2.subfields_as_dict()
                if feld1_sub.keys() == feld2_sub.keys()  and not all([str(f1_sub[0]).strip() == str(f2_sub[0]).strip() for f1_sub, f2_sub in zip(feld1_sub.values(), feld2_sub.values())]):
                    x = input('Old (0) or Merge (M)? New just enter: ')
                    if x in ['O', 'o']:
                        record.remove_field(feld1)
                        if feld2.tag == '260': feld2.tag = '264'
                        record.add_ordered_field(feld2)
                    if x == 'M':
                        print('Merge') 
                elif len(feld1_sub.keys()) < len(feld2_sub.keys()) :
                    if subset_keys(feld1_sub, feld2_sub):
                        record.remove_field(feld1)
                        if feld2.tag == '260': feld2.tag = '264'
                        record.add_ordered_field(feld2) 
                    else:             
                        x = input('This should be merged')
                        record.add_ordered_field(feld2)  
                elif not subset_keys(feld2_sub, feld1_sub):
                    x = input('This should be merged') 
                    record.add_ordered_field(feld2)      
    return record

def append_fields(record, record_trl, app_tags):
    for tag in app_tags:
        for field in record_trl.get_fields(tag):
            record.add_ordered_field(field)
    return record   

def merge_fields(record, record_trl):
    new = ['003', '040', '041']
    append = ['020','001', '500', '910', '998','700'] 
    rest = ['100', '240', '245', '260', '264', '300', '490', '595' ]
    record = append_fields(record, record_trl, append)
    new_008 = combine_008(record['008'].data, record_trl['008'].data)
    record.remove_fields('008') 
    record.add_ordered_field(Field(tag='008', indicators = [' ', ' '], data = new_008)) 
    record = decide_field(record, record_trl, rest)
    return record

def merge_records(OUT, schon_besetzt):
    possible_tags = set()
    with open(OUT , 'wb') as writer:
        for _, value in schon_besetzt.items():
            record = value[0]
            record_trl = value[1]
            feld_record_tag = set([x.tag for x in record.get_fields()])
            feld_record_tag.add('260')
            feld_trl_record_tag = set([x.tag for x in record_trl.get_fields()])
            common_tags = feld_record_tag.intersection(feld_trl_record_tag)
            input(common_tags)
            record = append_fields(record, record_trl, feld_trl_record_tag - feld_record_tag )
            record = merge_fields(record, record_trl)
            possible_tags.update(common_tags) 
            writer.write(record.as_marc())

        print(possible_tags)      


def iter_languages(lang, path_trl, path_lang, OUT):
    trl_lang  = select_languages_from_trl(lang = lang, path = path_trl)
    schon_besetzt = {}
    with open(OUT , 'wb') as writer:
        with open(path_lang, 'rb') as data:
            reader = MARCReader(data, to_unicode=True, force_utf8=True, utf8_handling="strict")
            for record in reader:
                if record.leader[7] == 'm':
                    for field in record.get_fields('100'):
                        subfields = field.subfields_as_dict()
                        if '7' in subfields.keys():
                            code = field['7']
                            if code in trl_lang.keys():
                                for record_trl in trl_lang[code]:
                                    if (record_trl.title.rstrip(" /:,") == record.title.rstrip(" /:,") or check_595(record_trl, record)) and check_year(record_trl, record):
                                        schon_besetzt = write_marc_dupl( writer, record, record_trl,  schon_besetzt)    

                        elif 'a' in subfields.keys():     
                            code = field['a']
                            if code in trl_lang.keys():
                                for record_trl in trl_lang[code]:
                                    if (record_trl.title.rstrip(" /:,") == record.title.rstrip(" /:,") or check_595(record_trl, record))and check_year(record_trl, record):
                                        schon_besetzt = write_marc_dupl( writer, record, record_trl,  schon_besetzt)
    return schon_besetzt   

def combine_duplicities(path_merged, path_lang, path_writer):
    merged_dict = {}
    with open(path_merged, 'rb') as merged:
        reader = MARCReader(merged, to_unicode=True, force_utf8=True, utf8_handling="strict")
        for record in reader:
            new_001 = record.get_fields('001')[0].data
            old_001 = record.get_fields('001')[1].data
            record.remove_field(record.get_fields('001')[0])
            merged_dict[new_001] = (old_001, record)
    with open(path_writer , 'wb') as writer:
        with open(path_lang, 'rb') as bibliography:
            reader = MARCReader(bibliography, to_unicode=True, force_utf8=True, utf8_handling="strict")
            for record in reader:
                if record['001'].data in merged_dict.keys():
                    writer.write(merged_dict[record['001'].data][1].as_marc())
                    continue
                for field in record.get_fields('994'):
                    if field['b'] in merged_dict.keys():
                        record.remove_field(field)
                        record.add_ordered_field(Field(tag = '994', indicators = [' ', ' '], subfields = [Subfield(code= 'a', value = 'UP'),
                                                                                                Subfield(code = 'b',value = merged_dict[field['b']][0])]))
                writer.write(record.as_marc())        






if __name__ == "__main__":
    lang = 'ita'
    schon_besetzt = iter_languages(lang, path_trl= 'data/ucla_trl.mrc', path_lang= f'data/marc_{lang}.mrc', OUT=f'data/duplicities_{lang}.mrc')
    merge_records(f'data/merged_{lang}.mrc', schon_besetzt)
    print(len(schon_besetzt.keys()))
    combine_duplicities(path_merged = f'data/merged_{lang}.mrc', path_lang = f'data/marc_{lang}.mrc', path_writer = f'data/marc_duplicities_{lang}.mrc')
