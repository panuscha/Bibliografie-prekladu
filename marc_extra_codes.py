import pymarc
from pymarc import Record, MARCReader, Subfield
import pandas as pd
import pickle



def import_czech_fennica(path):
    "Reads czech FENNICA marc records and saves them as DataFrame"
    df_dict = {}
    counter = 0
    key_set = set()
    with open(path, 'rb') as data:
        reader = MARCReader(data, to_unicode=True, force_utf8=True, utf8_handling="strict")
        for record in reader:
            counter += 1 
            for field in record.get_fields():
                key_set.add(field.tag)
    print(key_set)
    
    with open(path, 'rb') as data:
        reader = MARCReader(data, to_unicode=True, force_utf8=True, utf8_handling="strict")
        for record in reader:
            key_set_copy = key_set.copy()
            tag_001 = record['001'].data
            df_dict[tag_001] = {}
            print(record)
            for field in record.get_fields():
                if '$' in str(field):
                    df_dict[tag_001][field.tag] = df_dict[tag_001].get(field.tag, [])+ [field.subfields_as_dict()]
                else : 
                    df_dict[tag_001][field.tag] = df_dict[tag_001].get(field.tag, [])+ [str(field.data)]   
                # Delete key in key exists 
                if field.tag in key_set_copy : key_set_copy.remove(field.tag)
            df_dict[tag_001].update({field_tag: [] for field_tag in key_set_copy}) 

    print(counter)
    return df_dict

def create_dict_work(path):
    "Creates dictionary with authors - title:id pairs"
    # list of identifiers
    identifiers = []
    # dictionary with author - work:id pairs
    dict_author_work = {}
    with open(path, 'rb') as data:
        reader = MARCReader(data, to_unicode=True, force_utf8=True, utf8_handling="strict")
        for record in reader:
            if not record is None:
                for field in  record.get_fields('595') : 
                    if all([ x in str(field)  for x in ['$1', '$a', '$t']  ]): # if '$1' in str(field) and '$a' in str(field) and '$t' in str(field):
                        id = record['595']['1']
                        if not id in identifiers:
                            identifiers.append(id)
                        author = record['595']['a']
                        author = author[0:len(author)-1]
                        work = record['595']['t']
                        if not work is None: 
                            if work[-1] == '.':
                                work = work[0:len(work)-1]
                            if author.lower() in dict_author_work.keys():
                                dict_author_work[author.lower()].update({work.lower() : id })  
                            else:
                                dict_author_work[author.lower()] = {work.lower() : id } 

    return dict_author_work       

def import_bib_translations(path, lang):
    "Reads CLB translations marc records and saves lang-records as DataFrame" 
    count = 0
    strip_char = " ,./"
    df_dict = {'001': [],
                'title_trl': [],
               'author_name':[],
               'author_code':[],
               'finished':[]}
    with open(path, 'rb') as data, open("data/clb_trl_{lang}.mrc".format(lang =lang) , 'wb') as writer:
        reader = MARCReader(data, to_unicode=True, force_utf8=True, utf8_handling="strict")
        for record in reader:    
            if record['041']['a'] == lang: 
                writer.write(record.as_marc())
                df_dict['001'].append(str(record['001'].data))
                
                tags = ['author_name', 'author_code', 'title_trl', 'finished']
                for field in record.get_fields('100'):
                    df_dict['author_name'].append(field['a'].strip(strip_char) if '$a' in str(field) else '')
                    df_dict['author_code'].append(field['7'].strip(strip_char) if '$7' in str(field) else '') 
                    tags.remove('author_name')
                    tags.remove('author_code')
   
                for field in record.get_fields('245'):
                    df_dict['title_trl'].append(field['a'].strip(strip_char) if '$a' in str(field) else '')  
                    tags.remove('title_trl')
   
                for field in record.get_fields('915'):
                    df_dict['finished'].append('True' if '$a' in str(field) and field['a'].lower() == 'true'  else 'False')
                    tags.remove('finished')

                for tag in tags:
                    df_dict[tag].append('')

                count += 1
    print(count)
    return df_dict            
                              


if __name__ == "__main__":
    # df_dict = import_czech_fennica("data/fennica_preklady.mrc")
    # df = pd.DataFrame(df_dict).T
    # pickle.dump( df, open( "data/fennica_czech.obj", "wb" ) )
    lang = 'fin'
    df_dict = import_bib_translations("data/ucla_trl.mrc", lang)
    df = pd.DataFrame(df_dict)
    pickle.dump( df, open( "data/clb_trl_{lang}.obj".format(lang =lang), "wb" ) )
    print(df)
