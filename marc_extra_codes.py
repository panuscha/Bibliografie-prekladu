import pymarc
from pymarc import Record, MARCReader, Subfield, Reader
import pandas as pd
import pickle
import numpy as np
from collections import defaultdict

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

def create_dict_author_work(path, tag):
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
                    if all([ x in str(field)  for x in ['$1', '$t', '$'+tag]  ]): # if '$1' in str(field) and '$a' in str(field) and '$t' in str(field):
                        id = record['595']['1']
                        if not id in identifiers:
                            identifiers.append(id)
                        author = record['595'][tag]
                        author = author.strip(" ,.")
                        work = record['595']['t']
                        if not work is None: 
                            if author.lower() in dict_author_work.keys():
                                dict_author_work[author.lower()].update({work.lower() : id })  
                            else:
                                dict_author_work[author.lower()] = {work.lower() : id } 

    return dict_author_work   

def save_genre_audience(path):
    'Saves dictionary persistent ID: (audience, ) [22, 33]'
    df_work_database = pd.read_excel(path)
    for column in df_work_database.columns:
        df_work_database[column] = df_work_database[column].apply(lambda x: str(x))

    return { key: (audience, genre) for key, audience, genre in df_work_database[['perzistentní ID díla', 'publikum kód', 'hlavní žánr']] }
        

     

def create_dict_author_work_excel(path, author_column):
    "Creates dictionary with authors - title:id pairs"
    # list of identifiers
    identifiers = []
    # dictionary with author - work:id pairs
    dict_author_work = {}

    df_work_database = pd.read_excel(path)
    for column in df_work_database.columns:
        df_work_database[column] = df_work_database[column].apply(lambda x: str(x))

    

    # with open(path, 'rb') as data:
    #     reader = MARCReader(data, to_unicode=True, force_utf8=True, utf8_handling="strict")
    #     for record in reader:
    #         if not record is None:
    #             for field in  record.get_fields('595') : 
    #                 if all([ x in str(field)  for x in ['$1', '$t', '$'+tag]  ]): # if '$1' in str(field) and '$a' in str(field) and '$t' in str(field):
    for _, row in df_work_database.iterrows():
        id = row["perzistentní ID díla"]
        if not id in identifiers:
            identifiers.append(id)
        author = row[author_column].strip(" ,.")
        work = row["normalizovaný název"].strip(" ,.")
        if not work is None: 
            if author.lower() in dict_author_work.keys():
                dict_author_work[author.lower()].update({work.lower() : id })  
            else:
                dict_author_work[author.lower()] = {work.lower() : id } 

    return dict_author_work, identifiers   

   

def import_bib_translations(path, lang):
    "Reads CLB translations marc records and saves lang-records as DataFrame" 
    count = 0
    strip_char = " ,./"
    df_dict = {'001': [],
                'title_trl': [],
               'author_name':[],
               'author_code':[],
               'finished':[],
               'pub_year':[]}
    with open(path, 'rb') as data, open("data/clb_trl_{lang}.mrc".format(lang =lang) , 'wb') as writer:
        reader = MARCReader(data, to_unicode=True, force_utf8=True, utf8_handling="strict")
        for record in reader:    
            if record['041']['a'] == lang: 
                writer.write(record.as_marc())
                df_dict['001'].append(str(record['001'].data))

                df_dict['pub_year'].append(str(int(record['008'].data[7:11])))
                
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

def load_df_csv(path, language): 
    df = pd.read_csv(path, encoding='utf_8')       
    #df["Číslo záznamu"] = df["Číslo záznamu"].apply(lambda x: int(x) if not(pd.isnull(x)) else np.nan)
    df["Číslo záznamu"] = df["Číslo záznamu"].apply(lambda x: int(x) if not pd.isnull(x) else None)
    df["Číslo záznamu"] = df["Číslo záznamu"].astype("Int64")
    df['Typ záznamu'] = df['Typ záznamu'].apply(lambda x: str(x).strip() if not(pd.isnull(x)) else None)
    df['Je součást čeho (číslo záznamu)'] = df['Je součást čeho (číslo záznamu)'].apply(lambda x: int(x) if not(pd.isnull(x)) else None) #and str(x).isnumeric()
    df['typ díla (celé dílo, úryvek, antologie, souborné dílo)'] = df['typ díla (celé dílo, úryvek, antologie, souborné dílo)'].apply(lambda x: str(x).strip() if not(pd.isnull(x)) else None)
    df['Vztah k originálu (překlad vs. adaptace)'] = df['Vztah k originálu (překlad vs. adaptace)'].apply(lambda x: str(x).strip() if not(pd.isnull(x)) else None)
    df['Druh adaptace (slovem)'] = df['Druh adaptace (slovem)'].apply(lambda x: str(x).strip() if not(pd.isnull(x)) else None)
    df['Autor/ka + kód autority'] = df['Autor/ka + kód autority'].apply(lambda x: str(x).strip() if not(pd.isnull(x)) else None)
    df['Původní název'] = df['Původní název'].apply(lambda x: str(x).strip() if not(pd.isnull(x)) else None)
    df['Jazyk díla'] = df['Jazyk díla'].apply(lambda x: str(x).replace(';', ',').replace('§', ',').strip() if not(pd.isnull(x)) else None)
    df['Výchozí jazyk '] = df['Výchozí jazyk '].apply(lambda x: str(x).replace(';', ',').replace('§', ',').strip() if not(pd.isnull(x)) else None)
    df['Zprostředkovací jazyk'] = df['Zprostředkovací jazyk'].apply(lambda x: str(x).replace(';', ',').replace('§', ',').strip() if not(pd.isnull(x)) else None)
    df['Údaje o zprostředkovacím díle'] = df['Údaje o zprostředkovacím díle'].apply(lambda x: str(x).strip() if not(pd.isnull(x)) else None)
    df['Město vydání, země vydání, nakladatel'] = df['Město vydání, země vydání, nakladatel'].apply(lambda x: str(x).strip() if not(pd.isnull(x)) else None)
    df['Edice, svazek'] = df['Edice, svazek'].apply(lambda x: str(x).replace(',', ';').strip() if not(pd.isnull(x)) else None)
    df['Údaje o časopiseckém vydání'] = df['Údaje o časopiseckém vydání'].apply(lambda x: str(x).strip() if not(pd.isnull(x)) else None)
    df["Počet stran"] = df["Počet stran"].apply(lambda x: str(x).replace('.0','') if not(pd.isnull(x))  else None) #and str(x).isnumeric()
    df["Rok"] = df["Rok"].apply(lambda x: str(x).replace('.0','') if not(pd.isnull(x)) else None)
    df['ISBN'] = df['ISBN'].apply(lambda x: str(x).strip() if not(pd.isnull(x)) else None) 
    df['Volná poznámka'] = df['Volná poznámka'].apply(lambda x: str(x).strip() if not(pd.isnull(x)) else None)  
    df['technická poznámka'] = df['technická poznámka'].apply(lambda x: str(x).strip() if not(pd.isnull(x)) else None)  

    if language == 'fin': 
        df["Finské id"] = df["Finské id"].apply(lambda x: str(x).strip() if not(pd.isnull(x)) else None)
        df['Název díla dle titulu v latince'] = df['Název díla dle titulu v latince'].apply(lambda x: str(x).strip()if not(pd.isnull(x)) else None) 
        df['Údaje o odpovědnosti a další informace (z titulní strany)'] = df['Údaje o odpovědnosti a další informace (z titulní strany)'].apply(lambda x: str(x).strip()if not(pd.isnull(x)) else None)
        df['Další role'] = df['Další role'].apply(lambda x: str(x).strip() if not(pd.isnull(x)) else None) 

    if language == 'gre': 
        df['Název díla dle titulu v latince'] = df['Název díla dle titulu v latince'].apply(lambda x: str(x).strip()if not(pd.isnull(x)) else None) 
        df['Údaje o odpovědnosti a další informace (z titulní strany)'] = df['Údaje o odpovědnosti a další informace (z titulní strany)'].apply(lambda x: str(x).strip()if not(pd.isnull(x)) else None)
        df['Další role'] = df['Další role'].apply(lambda x: str(x).strip() if not(pd.isnull(x)) else None) 

    if language == 'ita':
        df['Název díla dle titulu (v příslušném písmu)'] = df['Název díla dle titulu (v příslušném písmu)'].apply(lambda x: str(x).strip()if not(pd.isnull(x)) else None) 
        df['Údaje o odpovědnosti a další informace'] = df['Údaje o odpovědnosti a další informace'].apply(lambda x: str(x).strip()if not(pd.isnull(x)) else None)
        df['Překladatel/ka'] = df['Překladatel/ka'].apply(lambda x: str(x).strip() if not(pd.isnull(x)) else None)
        df['Zdroj či odkaz'] = df['Zdroj či odkaz'].apply(lambda x: str(x).strip() if not(pd.isnull(x)) else None) 
    
    return df

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

def write_marc_dupl( writer, record, record_trl,  ret):
    author = record['100']['a']
    ret.append(f'{record.title} {author}')
    print(f'{record.title} {author}')
    writer.write(record.as_marc())
    writer.write(record_trl.as_marc())
    return ret


def iter_languages(lang, path_trl, path_lang, OUT):
    trl_lang  = select_languages_from_trl(lang = lang, path = path_trl)
    ret = []
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
                                    if record_trl.title.rstrip(" /:,") == record.title.rstrip(" /:,") and check_year(record_trl, record):
                                        ret = write_marc_dupl( writer, record, record_trl,  ret)    
                                    else:
                                        if check_595(record_trl, record) and check_year(record_trl, record):   
                                            ret = write_marc_dupl( writer, record, record_trl,  ret)


                        elif 'a' in subfields.keys():     
                            code = field['a']
                            if code in trl_lang.keys():
                                for record_trl in trl_lang[code]:
                                    if record_trl.title.rstrip(" /:,") == record.title.rstrip(" /:,") and check_year(record_trl, record):
                                        ret = write_marc_dupl( writer, record, record_trl,  ret)
                                    else:
                                        if check_595(record_trl, record) and check_year(record_trl, record):   
                                            ret = write_marc_dupl( writer, record, record_trl,  ret) 
    return ret                           


if __name__ == "__main__":
    lang = 'ita'
    ret = iter_languages(lang, path_trl= 'data/ucla_trl.mrc', path_lang= f'data/marc_{lang}.mrc', OUT=f'data/duplicities_{lang}.mrc')
    print(ret)
    print(len(ret))


# if __name__ == "__main__":
#     dict_author_code_work, identifiers = create_dict_author_work_excel("data/work-database_archive.xlsx", 'autor')
#     pickle.dump( dict_author_code_work, open( "data/dict_author_work.obj", "wb" ) )
#     pickle.dump( identifiers, open( "data/identifiers.obj", "wb" ) )