from pymarc import Record, MARCReader, Subfield, Reader, Field
from collections import defaultdict
from pymarc import Record, MARCReader, Subfield, map_xml

def autoitextu(record): 
    for field in record.get_fields('700'):
        subfields = field.subfields_as_dict()
        if '4' in subfields.keys():
            if 'autoitextu' in subfields['4']:
                record.remove_field(field)
                field['4'] = 'aut'
                record.add_ordered_field(field)
    return record  

def fra(record):
    for field in record.get_fields('041'):
        subfields = field.subfields_as_dict()
        for key, value in subfields.items():
            if 'fra' in value:
                record.remove_field(field)
                field[key] = 'fre'
                record.add_ordered_field(field)
    return record            

def add_240(record):
    for field in record.get_fields('240'):
        record.remove_field(field)
    for field in record.get_fields('595'):
        subfields = field.subfields_as_dict()
        if 't' in subfields.keys():
            work = field['t'].strip(' .') + '. '
            j = record['041']['a']
            if j == 'fin': jazyk = 'Finsky'
            if j == 'gre': jazyk = 'Řecky'
            if j == 'ita': jazyk = 'Italsky'
            if j == 'cze': jazyk = 'Česky'
            record.add_ordered_field(Field(tag = '240', indicators=[' ', ' '], subfields=[Subfield(code = 'a', value = work),
                                                                                    Subfield(code = 'l', value = jazyk)]))
    return record

def rec_gr23000690(record):
    for field in record.get_fields('994'):
        if field['a'] == 'UP' and field['b'] == 'gr23000690':
            for field in record.get_fields('041'):
                record.remove_field(field)
            record.add_ordered_field(Field(tag = '041', indicators=['1', ' '], subfields=[Subfield(code = 'a', value = 'gre'),
                                                                                    Subfield(code = 'h', value = 'cze')]))    
    return record

def UP(record, UP_DN):
    title = record['245']['a']
    for field in record.get_fields('994'):
        record.remove_field(field)
        code_001 = field['b']
        if len(code_001) == 11:
            code_001 = code_001[:5] + code_001[6:] 
        if len(code_001) == 9:
            code_001 = code_001[:5] + '0' + code_001[5:]     
        record.add_ordered_field(Field(tag = '994', indicators=[' ', ' '], subfields=[Subfield(code = 'a', value = field['a']),
                                                                                    Subfield(code = 'b', value = code_001),
                                                                                    Subfield(code = 'm', value = title),
                                                                                    Subfield(code = 'n', value = UP_DN[code_001])])) 
    return record

def mine_clo(path): 
    codes_clo = {}
    with open(path, 'rb') as data:
        reader = MARCReader(data, to_unicode=True, force_utf8=True, utf8_handling="strict")
        for i, record in enumerate(reader):
            notskip = True
            for field in record.get_fields('999'):
                notskip= False
            if notskip:
                for field in record.get_fields('100'):
                    subfields = field.subfields_as_dict()
                    if '7' in subfields:
                        codes_clo[field['7']] = field['a']
    return codes_clo                



def clo(record, codes_clo):
    for field in record.get_fields('100'):
        subfields = field.subfields_as_dict()
        if '7' in subfields:
            c = field['7']
            name = field['a']
            if c in codes_clo.keys() and name.strip(', ') != codes_clo[c].strip(', '):
                new_name = codes_clo[c].strip(', ') + ', ' 
                print(f'Old name : {name}; new name: {new_name}')
                record.remove_field(field)
                field['a'] = new_name
                record.add_ordered_field(field)
    return record            

             







if __name__ == '__main__':
    OUT = 'data/marc_bibliografie_prekladu_a_d_add_trl_2.mrc'
    UP_DN = {}
    codes_clo = mine_clo('data/clo_20250422.mrc')
    with open(OUT , 'wb') as writer:
        with open(f'data/marc_bibliografie_prekladu_a_d_add_trl.mrc', 'rb') as data:
            reader = MARCReader(data, to_unicode=True, force_utf8=True, utf8_handling="strict")
            for i, record in enumerate(reader):
                UP_DN[record['001'].data] = record['245']['a'] 
                record = autoitextu(record)
                record = fra(record)
                record = add_240(record)
                record = rec_gr23000690(record)
                record = clo(record, codes_clo)
                writer.write(record.as_marc())  
    OUT = 'data/marc_bibliografie_prekladu_a_d_add_trl_3.mrc'            
    with open(OUT , 'wb') as writer:
        with open(f'data/marc_bibliografie_prekladu_a_d_add_trl_2.mrc', 'rb') as data:
            reader = MARCReader(data, to_unicode=True, force_utf8=True, utf8_handling="strict")
            for i, record in enumerate(reader):
                record = UP(record, UP_DN)
                record['964']['a'] = 'TRL'                                
                writer.write(record.as_marc())             