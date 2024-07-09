import pymarc
from pymarc import Record, MARCReader, Subfield
import pandas as pd
import pickle
import numpy as np

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

    return dict_author_work   

   

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

def load_df_csv(path): 
    df = pd.read_csv(path, encoding='utf_8')       
    df["Číslo záznamu"] = df["Číslo záznamu"].apply(lambda x: int(x) if not(pd.isnull(x)) else np.nan)
    df["Finské id"] = df["Finské id"].apply(lambda x: str(x).strip() if not(pd.isnull(x)) else np.nan)
    df['Typ záznamu'] = df['Typ záznamu'].apply(lambda x: str(x).strip() if not(pd.isnull(x)) else np.nan)
    df['Je součást čeho (číslo záznamu)'] = df['Je součást čeho (číslo záznamu)'].apply(lambda x: int(x) if not(pd.isnull(x)) else np.nan) #and str(x).isnumeric()
    df['typ díla (celé dílo, úryvek, antologie, souborné dílo)'] = df['typ díla (celé dílo, úryvek, antologie, souborné dílo)'].apply(lambda x: str(x).strip() if not(pd.isnull(x)) else np.nan)
    df['Vztah k originálu (překlad vs. adaptace)'] = df['Vztah k originálu (překlad vs. adaptace)'].apply(lambda x: str(x).strip() if not(pd.isnull(x)) else np.nan)
    df['Druh adaptace (slovem)'] = df['Druh adaptace (slovem)'].apply(lambda x: str(x).strip() if not(pd.isnull(x)) else np.nan)
    df['Autor/ka + kód autority'] = df['Autor/ka + kód autority'].apply(lambda x: str(x).strip() if not(pd.isnull(x)) else np.nan)
    df['Název díla dle titulu v latince'] = df['Název díla dle titulu v latince'].apply(lambda x: str(x).strip()if not(pd.isnull(x)) else np.nan) 
    df['Údaje o odpovědnosti a další informace (z titulní strany)'] = df['Údaje o odpovědnosti a další informace (z titulní strany)'].apply(lambda x: str(x).strip()if not(pd.isnull(x)) else np.nan) 
    df['Původní název'] = df['Původní název'].apply(lambda x: str(x).strip() if not(pd.isnull(x)) else np.nan)
    df['Jazyk díla'] = df['Jazyk díla'].apply(lambda x: str(x).strip() if not(pd.isnull(x)) else np.nan)
    df['Výchozí jazyk '] = df['Výchozí jazyk '].apply(lambda x: str(x).strip() if not(pd.isnull(x)) else np.nan )
    df['Zprostředkovací jazyk'] = df['Zprostředkovací jazyk'].apply(lambda x: str(x).strip() if not(pd.isnull(x)) else np.nan)
    df['Údaje o zprostředkovacím díle'] = df['Údaje o zprostředkovacím díle'].apply(lambda x: str(x).strip() if not(pd.isnull(x)) else np.nan)
    df['Město vydání, země vydání, nakladatel'] = df['Město vydání, země vydání, nakladatel'].apply(lambda x: str(x).strip() if not(pd.isnull(x)) else np.nan)
    df['Edice, svazek'] = df['Edice, svazek'].apply(lambda x: str(x).strip() if not(pd.isnull(x)) else np.nan )
    df['Údaje o časopiseckém vydání'] = df['Údaje o časopiseckém vydání'].apply(lambda x: str(x).strip() if not(pd.isnull(x)) else np.nan)
    df["Počet stran"] = df["Počet stran"].apply(lambda x: str(int(x)) if not(pd.isnull(x)) else np.nan)
    df["Rok"] = df["Rok"].apply(lambda x: str(int(x)) if not(pd.isnull(x)) else np.NaN)
    df['ISBN'] = df['ISBN'].apply(lambda x: str(x).strip() if not(pd.isnull(x)) else np.nan) 
    df['Další role'] = df['Další role'].apply(lambda x: str(x).strip() if not(pd.isnull(x)) else np.nan)   
    df['Volná poznámka'] = df['Volná poznámka'].apply(lambda x: str(x).strip() if not(pd.isnull(x)) else np.nan)  
    df['technická poznámka'] = df['technická poznámka'].apply(lambda x: str(x).strip() if not(pd.isnull(x)) else np.nan)  
    return df

                              


if __name__ == "__main__":
    # df_dict = import_czech_fennica("data/fennica_preklady.mrc")
    # df = pd.DataFrame(df_dict).T
    # pickle.dump( df, open( "data/fennica_czech.obj", "wb" ) )
    # lang = 'fin'
    # df_dict = import_bib_translations("data/ucla_trl.mrc", lang)
    # df = pd.DataFrame(df_dict)
    # pickle.dump( df, open( "data/clb_trl_{lang}.obj".format(lang =lang), "wb" ) )
    # print(df)
    dict_author_code_work = create_dict_author_work_excel("data/work-database_archive.xlsx", 'autor')
    pickle.dump( dict_author_code_work, open( "data/dict_author_work.obj", "wb" ) )