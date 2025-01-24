from marc_bibliografie_prekladu_abc import Bibliografie_record
from abc import ABC, abstractmethod 
from pymarc import Record, Subfield
import pandas as pd
from pymarc.field import Field
import re
import pickle 
import numpy as np
import marc_extra_codes

class Bibliografie_record_gre(Bibliografie_record):
    def __init__(self, finalauthority_path, dict_author_work, dict_author__code_work_path):
        super(Bibliografie_record_gre, self).__init__(finalauthority_path, dict_author_work, dict_author__code_work_path)
        self.greek_articles =  ['ένα', 'έναν', 'ένας', 'ενός','η','μια','μια(ν)','μιας','ο','οι', 'τα','τη(ν)','της','τις','το','τον','του','τους','των']  ## TODO: ADD GREEK ARTICLES
        self.tag = 'gr23' 


    ## TODO: NA TO SE UŽ NEDÁ KOUKAT PŘEPSAT    
    def get_translators_and_other_roles(self, other_roles, record):
        # Regex pattern to match text between parentheses  
        pattern_role = r"\((.*?)\)"
        pattern_name = r".*(?=\s+\()"
        pattern_clean = r"[^a-zA-Z]"
        translators = ''
        t = other_roles
        while '§' in other_roles: ## TODO: PREPSAT
            start = other_roles.find('§') 
            t = other_roles[:start ].strip()
            # Search for the first occurrence of the pattern
            match_role = re.search(pattern_role, t)
            match_name = re.search(pattern_name, t)
            if match_role and match_name:
                name = match_name.group(0)
                roles = match_role.group(1)
                roles = roles.split(",")
                for role in roles:
                    if 'trl' in role:
                        if translators != '':
                            translators += ' § ' + name 
                        else:
                            translators +=  name     
                    else:
                        clean_role = re.sub(pattern_clean, "", role)
                        record.add_ordered_field(Field(tag='700', indicators=['1',' '], subfields=[Subfield(code='a', value= name),
                                                                                    Subfield(code='4', value= clean_role),])) 
            other_roles = other_roles[start+1: ]              
        t = other_roles.strip()
        # Search for the first occurrence of the pattern
        match_role = re.search(pattern_role, t)
        match_name = re.search(pattern_name, t)
        if match_role and match_name:
            name = match_name.group(0)
            roles = match_role.group(1)
            roles = roles.split(",")
            for role in roles:
                if 'trl' in role:
                    if translators != '':
                        translators += ' § ' + name 
                    else:
                        translators +=  name 
                else:
                    clean_role = re.sub(pattern_clean, "", role)
                    record.add_ordered_field(Field(tag='700', indicators=['1',' '], subfields=[Subfield(code='a', value= name),
                                                                                    Subfield(code='4', value= clean_role),])) 
        print(translators)            
        if translators != '': return translators, record 
        else: return None, record              
        
    
    def calculate_articles(self, row):
        title = str(row["Název díla v původním písmu"])
        # matches first word in string
        first_word = re.search('^([\w]+)', title)
        if not first_word is None: 
            # matches first word in string
            first_word = re.search('^([\w]+)', title).group(0)
            if first_word.lower() in self.greek_articles:
                skip = str(len(first_word) + 1)
            else:
                skip = '0'    
        else:
            skip = '0'
        return skip 
        
    def add_245(self, row, title, subtitle,  record):
        """Adds data to subfield 245. 
        Finds if work's title starts with an article -> writes how many positions the article takes
        """
        skip = self.calculate_articles(row)
        ### DODELAT
        c = '' 
        if not pd.isnull(row['Údaje o odpovědnosti a další informace (z titulní strany)']):
            c = row['Údaje o odpovědnosti a další informace (z titulní strany)']       
        title = title.strip()     
        if subtitle == '' and c == '':                                                                          
            record.add_ordered_field(Field(tag = '245', indicators = ['0', skip], subfields = [Subfield(code='a', value= title + "."), ]))                                                                          
        else:
            if c == '':
                subtitle = subtitle.strip()
                record.add_ordered_field(Field(tag = '245', indicators = ['0', skip], subfields = [Subfield(code='a', value= title + " : "), 
                                                                                        Subfield(code='b', value= subtitle + "."),]))
            elif subtitle == '':  
                c = c.strip()    
                record.add_ordered_field(Field(tag = '245', indicators = ['1', skip], subfields = [Subfield(code='a', value= title + " / "),
                                                                                                Subfield(code='c', value= c)]))
            else:
                subtitle = subtitle.strip()
                c = c.strip() 
                record.add_ordered_field(Field(tag = '245', indicators = ['1', skip], subfields = [Subfield(code='a', value= title + " : "), 
                                                                                        Subfield(code='b', value= subtitle + " / "),
                                                                                        Subfield(code='c', value= c)]))
        return record   
    
         

    def add_880(self, row, record):
        """links latin and greek script together
        název díla v původním písmu - 245, nakladatel v pův. písmu - 264, název edice - 490, další role - 700, název časopisu - 773 
        """
        fields_codes = {'245': 'Název díla v původním písmu',  # can have subtitle - then in subfield b 
                        '264': 'Nakadatel v původním písmu', # in subfield 'b'
                        '490': 'Název edice v původním písmu',
                        '700': 'Další role v původním písmu', 
                        '773': 'Název časopisu v původním písmu'} # is in subfield t
        last_index = 1
        has_code = set()
        for code in fields_codes.keys():
            for field in record.get_fields(code):
                has_code.add(code)
        for code, column in fields_codes.items():
            if not pd.isnull(row[column]) and code in has_code:
                l = last_index
                if code  == '700':
                    n_o_roles = [] 
                    code_700 = True
                    previous_name = 'None'
                else:
                    code_700 = False 
                    n_o_roles = [1]    
                    
                for field in record.get_fields(code):

                    subfields = field.subfields_as_dict()
                    field.add_subfield(code = '6', value= '{code}-0{last}/(S'.format(code = code, last = str(l)) )
                    
                    for subfield_code, subfield_value in subfields.items():
                        field.delete_subfield(subfield_code)
                        field.add_subfield(code = subfield_code, value = subfield_value[0])
                        if code_700 and subfield_code == 'a':
                            if subfield_value == previous_name:
                                n_o_roles[-1] += 1
                            else:
                                n_o_roles.append(1) 
                                previous_name = subfield_value   
                    l = l+1

                for value in row[column].split('§'):
                    
                    for _ in range(n_o_roles[0]):
                        ind1 = record[code].indicator1
                        ind2 = record[code].indicator2
                        if code == '245' and value.find(':') != -1:
                            title = value.split(':')[0]
                            subtitle = value.split(':')[1]
                            record.add_ordered_field(Field(tag='880', indicators = [ind1, ind2], subfields = [Subfield(code='6', value= '{code}-0{last}/(S'.format(code = code, last = str(last_index))),
                                                                                        Subfield(code='a', value= title + " :"),
                                                                                        Subfield(code='b', value= subtitle + " .") ])) 
                        
                        elif code == '264':
                            record.add_ordered_field(Field(tag='880', indicators = [ind1, ind2], subfields = [Subfield(code='6', value= '{code}-0{last}/(S'.format(code = code, last = str(last_index))),
                                                                                        Subfield(code='b', value= value.strip()), ]))    
                        elif code == '773':
                            record.add_ordered_field(Field(tag='880', indicators = [ind1, ind2], subfields = [Subfield(code='6', value= '{code}-0{last}/(S'.format(code = code, last = str(last_index))),
                                                                                        Subfield(code='t', value= value.strip()), ]))  
                        else:    
                            record.add_ordered_field(Field(tag='880', indicators = [ind1, ind2], subfields = [Subfield(code='6', value= '{code}-0{last}/(S'.format(code = code, last = str(last_index))),
                                                                                        Subfield(code='a', value= value.strip()), ]))
                        last_index = last_index+1
                    if code_700: n_o_roles.pop(0)   
        return record
    
    
    def add_common_specific(self, row, record):
        " 001, 240, title, subtitle, liability -> 245"    
       
        if not(pd.isnull(row['Původní název'])) and not (("originál neznámý" in str(row['Původní název']).lower())  or ("originál neexistuje" in str(row['Původní název']).lower())):
            original_title = row['Původní název'].strip()  
            record.add_ordered_field(Field(tag='240', indicators = ['1', '0'], subfields = [Subfield(code='a', value= original_title),
                                                                                        Subfield(code='l', value= 'řecky'), ]))
            
        if not(pd.isnull(row['Zdroj či odkaz'])) and not (row['Zdroj či odkaz'] == ' '):
            record.add_ordered_field(Field(tag = '998', indicators=[' ', ' '], subfields=[Subfield(code='a', value= row['Zdroj či odkaz'].strip()),] ) )    
            
        (title, subtitle) = self.get_title_subtitle(str(row['Název díla dle titulu v latince']))    
        record = self.add_245(row, title, subtitle,  record)    
         
        return record 
  
    
    def create_record_part_of_book(self, row, df):
        """Creates record for part of the book.
        Adds all fields that are specific to parts of book"""
        record = Record(to_unicode=True,
            force_utf8=True)
        record.leader = '-----naa---------4i-4500'  
        ind = row['Je součást čeho (číslo záznamu)']
        book_row = df.loc[df['Číslo záznamu'] == ind]
        
        # from Dataframe to Pandas Series
        book_row = book_row.squeeze()

        # is the author same as in the collective work, or does the book has it's own author 
        if pd.isnull(row['Autor/ka + kód autority']):
            tup, record = self.add_author_code(book_row['Autor/ka + kód autority'], record)
        else:
            tup, record  = self.add_author_code(row['Autor/ka + kód autority'], record)
        author = tup[0] 
        code = tup[1]    
  
        if not pd.isnull(book_row['Další role']):
            translators, record = self.get_translators_and_other_roles(book_row['Další role'], record)  ##  TODO: PREPSAT TAK ABY SOUHLASILO 880
        else:
            translators = None    

        if not pd.isnull(book_row['Edice, svazek']):    
            record = self.add_490(book_row['Edice, svazek'], record)    

        if pd.isnull(book_row['Město vydání, země vydání, nakladatel']):
            publication_country = 'xx-'

        else:
            publication = book_row['Město vydání, země vydání, nakladatel'] 
            matches = re.findall(r'\(\s*(\w+)\s*\)', publication)

            if len(matches) == 1:
                country =  matches[0]
                if country == 'Řecko': publication_country = 'gr-'
                elif country == 'Česká republika': publication_country = 'xr-'
                else : publication_country = 'xx-'         
            elif len(matches) == 2 : 
                if matches[0] == matches[1]: publication_country = 'gr-'
                else:  publication_country = 'vp-'
            else: publication_country = 'xx-'    
        
        record = self.add_008(book_row, record, publication_country, 'mul' if ',' in book_row['Jazyk díla'] else 'gre')      
        record = self.add_041(book_row, record) 
        record = self.add_264(book_row, record)
        record = self.add_commmon(row, record, author, code, translators)
        record = self.add_common_specific(row, record)    
        record = self.add_994_part_of_book(row, record)
        book_row['Název díla v původním písmu'] = row['Název díla v původním písmu']
        record = self.add_880(book_row, record)  
        return record


    def create_record_book(self, row, df):
        """Creates record for book.
        Adds all fields that are specific to books"""
        record = Record(to_unicode=True,
            force_utf8=True)
        record.leader = '-----nam---------4i-4500'
        tup, record = self.add_author_code(row['Autor/ka + kód autority'], record)
        author = tup[0]
        code = tup[1]

        if not pd.isnull(row['Další role']):
            translators, record = self.get_translators_and_other_roles(row['Další role'] , record)  
        else:
            translators = None   
            
        record = self.add_041(row, record)  
           
        if not pd.isnull(row['Edice, svazek']):    
            record = self.add_490(row['Edice, svazek'], record)     

        if pd.isnull(row['Město vydání, země vydání, nakladatel']):
            publication_country = 'xx-'

        else:
            publication = row['Město vydání, země vydání, nakladatel'] 
            matches = re.findall(r'\(\s*(\w+)\s*\)', publication)

            if len(matches) == 1:
                country =  matches[0]
                if country == 'Řecko': publication_country = 'gr-'
                elif country == 'Česká republika': publication_country = 'xr-'
                else : publication_country = 'xx-'         
            elif len(matches) == 2 : 
                if matches[0] == matches[1]: publication_country = 'gr-'
                else:  publication_country = 'vp-'
            else: publication_country = 'xx-'        
            
        record = self.add_008(row, record, publication_country, 'mul' if ',' in row['Jazyk díla'] else 'gre')
        record = self.add_commmon(row, record, author, code, translators) 
        record = self.add_264(row, record)
        record = self.add_common_specific(row, record)      
        
        #if  row['typ díla (celé dílo, úryvek, antologie, souborné dílo)'] in ['souborné dílo', 'antologie']:
        record = self.add_994_book(row, df, record)   
            
        record = self.add_880(row, record)        
        return record

    def create_article(self, row):
        """Creates record for the article.
        Adds all fields that are specific to articles."""
        record = Record(to_unicode=True,
            force_utf8=True)
        record.leader = '-----nab---------4i-4500' 
        tup, record = self.add_author_code(row['Autor/ka + kód autority'], record)
        author = tup[0]
        code = tup[1]
        if not pd.isnull(row['Další role']):
            translators, record = self.get_translators_and_other_roles(row['Další role'] , record)  
        else:
            translators = None   
        
        if  pd.isnull(row['Zprostředkovací jazyk']) or len(row['Zprostředkovací jazyk']) < 2:                                                                          
            record.add_ordered_field(Field(tag='041', indicators=['1',' '],subfields=[Subfield(code='a', value= re.search('[^\s]+', str(row['Jazyk díla'])).group(0)), 
                                                                                    Subfield(code='h', value= re.search('[^\s]+', str(row['Výchozí jazyk '])).group(0)),])) 
        else:
            record.add_ordered_field(Field(tag='041', indicators=['1',' '],subfields=[Subfield(code='a', value= re.search('[^\s]+', str(row['Jazyk díla'])).group(0)), 
                                                                                    Subfield(code='h', value= re.search('[^\s]+', str(row['Výchozí jazyk '])).group(0)),
                                                                                    Subfield(code='k', value= re.search('[^\s]+', str(row['Zprostředkovací jazyk'])).group(0)),]) )    
                
        if pd.isnull(row['Město vydání, země vydání, nakladatel']):
            publication_country = 'xx-'

        else:
            publication = row['Město vydání, země vydání, nakladatel'] 
            start = publication.find('(')+1
            end = publication.find(')') 
            country = publication[start:end]
            if country == 'Česká republika':
                publication_country = 'xr-'
            elif country == 'Řecko':
                publication_country = 'gr-'    ## GREECE - > GR ????      
            else:
                publication_country = 'xx-'
        
        record = self.add_008(row, record, publication_country, "gre") 
        record = self.add_commmon(row, record, author, code, translators)
        record = self.add_773(record, row)
        record = self.add_common_specific(row, record)  
        record = self.add_880(row, record)  
        return record 
    
  
        
 
    

if __name__ == "__main__":
    
    # file with all czech translations and their id's 
    czech_translations="data/czech_translations_full_18_01_2022.mrc" # obohacovat o kody 595
    # list of identifiers
    identifiers = []

    dict_author_work_path = "data/dict_author_work.obj"

    dict_author__code_work_path = "data/dict_author_code_work.obj"
    # initial table 
    IN = 'data/preklady/Bibliografie_prekladu_gre.csv'
    # final file
    OUT = 'data/marc_gre.mrc'

    df = marc_extra_codes.load_df_csv(IN, 'gre')
    

    # table with authority codes
    finalauthority_path = 'data/finalauthority_simple.csv'

    err = []  
    # writes data to file in variable OUT
    file = open("data/dict_author_work.obj",'rb')
    dict_author_work = pickle.load(file)  
        
    with open(OUT , 'wb') as writer:
        #iterates all rows in the table
        for index, row in df.iterrows():
            try:
                if isinstance(row['Typ záznamu'], str):
                    print(row['Číslo záznamu'])
                    print(row['Typ záznamu'])
                    bib_gre = Bibliografie_record_gre(finalauthority_path = finalauthority_path, dict_author_work= dict_author_work_path, \
                                                      dict_author__code_work_path = dict_author__code_work_path)
                    if 'kniha' in row['Typ záznamu']: 
                        record = bib_gre.create_record_book(row, df)
                    if 'část knihy' in row['Typ záznamu']: 
                        record = bib_gre.create_record_part_of_book(row, df)
                    if 'článek v časopise' in row['Typ záznamu']:
                        record = bib_gre.create_article(row)
                    print(record)
                    dict_author_work = bib_gre.dict_author_work
                    pickle.dump( bib_gre.dict_author_work, open( "data/dict_author_work.obj", "wb" ) )
                    pickle.dump( bib_gre.dict_author_code_work, open( "data/dict_author_code_work.obj", "wb" ) )
                    writer.write(record.as_marc())
            except :
                err.append(row['Číslo záznamu'])
    with open('data/dict_author_work.obj', 'wb') as f: 
        pickle.dump(dict_author_work, f)             
    print(err)           
    writer.close()
    