from pymarc import Record, MARCReader, Subfield, Reader, Field

def fix_008(record):
    data_008 = record['008'].data
    delete = record['008'] 
    if len(data_008) != 40: 
        print(data_008)
        record.remove_field(delete)
        fixed_008 = data_008.replace(']', '').replace('[', '')
        fixed_008 = fixed_008.replace('nan', '-')
        if fixed_008[12].isnumeric():
            fixed_008 = fixed_008[:6] + 'm' + fixed_008[7:]
            if fixed_008[13].isnumeric():
                 insert = fixed_008[7:9] + fixed_008[12:14]
                 fixed_008 =  fixed_008[:11] + insert + fixed_008[18:]
            else: 
                 insert = fixed_008[7:10] + fixed_008[12]   
                 fixed_008 =  fixed_008[:11] + insert + fixed_008[17:] 
        print(fixed_008)         
        record.add_ordered_field(Field(tag='008', indicators = [' ', ' '], data = fixed_008))
    return record

def fix_author(record): 
    for field in record.get_fields('100'):
        subfields = field.subfields_as_dict()
        if 'a' in subfields.keys() and '?' in subfields['a'][0]:
            record.remove_field(field)
        else:
            record['100']['a'] = record['100']['a'].strip()        
    return record        

def fix_240(record):
    record['240']['l'] = record['240']['l'].capitalize()
    return record

def add_characters(string):
    string = string.strip()
    if 'traduzione di' in string: return f'traduzione di <<{string[14:]}>>'  
    if 'a cura di' in string.lower():  return f'a cura di <<{string[9:]}>>'
    return f'<<{string}>>'
    

def fix_245_c(record):
    if record['001'].data[:2] == 'it':
        for field in record.get_fields('245'):
            subfields = field.subfields_as_dict()
            if 'c' in subfields.keys():
                old_traduzione = subfields['c'][0].split(';')
                if len(old_traduzione) > 1: 
                    name = old_traduzione[0].strip()
                    rest = old_traduzione[1].split(', ')
                    rest = ', '.join([add_characters(a) for a in rest])
                    new_traduzione = f'<<{name}>> ; {rest}'
                elif len(old_traduzione) == 1:
                    name = old_traduzione[0].strip()
                    new_traduzione = f'<<{name}>>' 
                print(new_traduzione)
    return record        

def fix_250(record):
    record['250']['a'] = record['250']['a'].capitalize()
    return record

def fix_490(record):
    for field in record.get_fields('490'):
        subfields = field.subfields_as_dict()
        if 'v' in subfields.keys() and 'pořadí neuvedeno'in subfields['v'][0]:
            field.delete_subfield('v')
            if 'a' in subfields.keys(): record['490']['a'] = record['490']['a'].strip(' ;,') 
    return record        
        

if __name__ == "__main__":
    OUT = 'data/marc_bibliografie_prekladu_a_d.mrc'
    with open(OUT , 'wb') as writer:
        with open(f'data/marc_bibliografie_prekladu_combined copy.mrc', 'rb') as data:
            reader = MARCReader(data, to_unicode=True, force_utf8=True, utf8_handling="strict")
            for i, record in enumerate(reader): 
                if i < 3011:
                    record = fix_008(record)
                    record = fix_author(record)
                    for field in record.get_fields('240'):
                        record = fix_240(record)
                    record = fix_245_c(record)    
                    for field in record.get_fields('250'):
                        record = fix_250(record)  
                    record = fix_490(record)       
                    writer.write(record.as_marc())
                else: 
                    break      