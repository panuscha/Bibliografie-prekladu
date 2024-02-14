from marc_bibliografie_prekladu_abc import Bibliografie_record
from abc import ABC, abstractmethod 
from pymarc import Record, MARCReader, Subfield
import pandas as pd
from pymarc.field import Field
from datetime import datetime
import re
import random
import pickle

class Bibliografie_record_fin(Bibliografie_record): 
    
    def __init__(self, finalauthority, dict_author_work, identifiers, fennica, clb_trl):
        self.finalauthority = finalauthority
        self.dict_author_work = dict_author_work
        self.identifiers = identifiers 
        self.df_fennica = pickle.load(open(fennica, "rb" ))
        self.clb_trl = pickle.load(open(clb_trl, "rb" ))
        
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
        
    
    def add_008(self, row, record):
        """Creates fixed length data and adds them to field 008 """ 
        date_record_creation = str(datetime.today().strftime('%y%m%d'))
        letter = 's'

        if pd.isnull(row['Rok']):
            publication_date = '--------'
        else:
            publication_date = str(int(row['Rok']))+ '----' 

        if pd.isnull(row['Město vydání, země vydání, nakladatel']):
            publication_country = 'xx-'

        else:
            publication = row['Město vydání, země vydání, nakladatel'] 
            start = publication.find('(')+1
            end = publication.find(')') 
            country = publication[start:end]
            if country == 'Finsko':
                publication_country = 'fi-'
            elif country == 'Česká republika':
                publication_country = 'xr-'    
            else:
                publication_country = 'xx-'

        material_specific =  '-----------------'
        language = 'fin'
        modified = '-'
        cataloging_source = 'd'
        data = date_record_creation + letter + publication_date +  publication_country + material_specific + language + modified + cataloging_source
        record.add_ordered_field(Field(tag='008', indicators = [' ', ' '], data = data))
        return record
    

    def add_035(self, row, record ):
        "Adds FENNICA catalogue number to 035$a"
        finnish_id = row["Finské id"]
        record.add_ordered_field(Field(tag='035', indicators = [' ', ' '], subfields = [Subfield(code='a', value= "(FENNICA)[{fennica}]".format(fennica = finnish_id))]))
        return record
    
    def add_998(self, row, record ):
        "Adds Melinda hypertext to 998$a"
        finnish_id = row["Finské id"]
        record.add_ordered_field(Field(tag='998', indicators = [' ', ' '], subfields = [Subfield(code='a', value= "https://melinda.kansalliskirjasto.fi/byid/{fennica}".format(fennica = finnish_id))]))
        return record

        
       
    def add_245(self,row, liability, title, subtitle, author, translators,  record):
        """Adds data to subfield 245. 
        Finds if work's title starts with an article -> writes how many positions the article takes
        """
                # matches first word in string
        skip = '0'
        c = ''
        if not pd.isnull(row['Údaje o odpovědnosti a další informace (z titulní strany)']):
            c = row['Údaje o odpovědnosti a další informace (z titulní strany)']      # Údaje o odpovědnosti a další informace
        title = title.strip()      
        if subtitle == '' and c == '':                                                                          
            record.add_ordered_field(Field(tag = '245', indicators = ['0', skip], subfields = [Subfield(code='a', value= title + " ."), ]))                                                                          
        else:
            if c == '':
                subtitle = subtitle.strip()
                record.add_ordered_field(Field(tag = '245', indicators = ['0', skip], subfields = [Subfield(code='a', value= title + " :"), 
                                                                                        Subfield(code='b', value= title + " ."),]))
            elif subtitle == '':  
                c = c.strip()    
                record.add_ordered_field(Field(tag = '245', indicators = ['1', skip], subfields = [Subfield(code='a', value= title + " /"),
                                                                                                Subfield(code='c', value= c)]))
            else:
                subtitle = subtitle.strip()
                c = c.strip() 
                record.add_ordered_field(Field(tag = '245', indicators = ['1', skip], subfields = [Subfield(code='a', value= title + " :"), 
                                                                                        Subfield(code='b', value= title + " /"),
                                                                                        Subfield(code='c', value= c)]))
        return record        
                
    def add_common_specific(self, row, record, author, translators):
        " 001, 035, 240, title, subtitle, liability -> 245"
        record.add_ordered_field(Field(tag='001', indicators = [' ', ' '], data=str('fi24'+ "".join(['0' for a in range(6-len(str(row['Číslo záznamu'])))]) + str(row['Číslo záznamu'])))) 
        record = self.add_035(row, record)
        
        if not(pd.isnull(row['Původní název'])) and not (("originál neznámý" in str(row['Původní název']).lower())  or ("originál neexistuje" in str(row['Původní název']).lower())):
            original_title = row['Původní název'].strip()                                                                       
            record.add_ordered_field(Field(tag='240', indicators = ['1', '0'], subfields = [Subfield(code='a', value= original_title),
                                                                                        Subfield(code='l', value= 'finsky'), ])) 
            
        (title, subtitle) = self.get_title_subtitle(str(row['Název díla dle titulu v latince']))
        liabiliy = row['Údaje o odpovědnosti a další informace (z titulní strany)']
        record = self.add_245(row, liabiliy, title, subtitle, author, translators,record) 
        record = self.add_998(row, record)
        return record 
     
    def add_994_book(self, row, df, record):
        """Adds id's of all parts of the collective work to field 994."""
        cislo_zaznamu = row['Číslo záznamu']
        is_part_of = df['Je součást čeho (číslo záznamu)']==cislo_zaznamu
        if any(is_part_of):
            book_rows = [i for i, val in enumerate(is_part_of) if val]
            for i in book_rows:
                r = df.iloc[i]
                number = "fi24"+"".join(['0' for a in range(6-len(str(row['Číslo záznamu'])))]) + str(r['Číslo záznamu'])
                record.add_ordered_field(Field(tag = '994', indicators = [' ', ' '], subfields = [Subfield(code= 'a', value = 'DN'),
                                                                                                Subfield(code = 'b',value = number)] ))   
        return record             
    
    def add_994_part_of_book(self, row, record):
        """Adds id of the collective work to field 994."""
        is_part_of = str(int(row['Je součást čeho (číslo záznamu)']))
        number = "fi22"+"".join(['0' for a in range(6-len(is_part_of))]) + is_part_of
        record.add_ordered_field(Field(tag = '994', indicators = [' ', ' '], subfields = [Subfield(code= 'a', value = 'UP'),
                                                                                                Subfield(code = 'b',value = number)]))
        return record
    
    def create_record_book(self,row, df):
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
          
        record = self.add_008(row, record)
        record = self.add_commmon(row, record, author, code, translators)  
        record = self.add_common_specific(row, record, author, translators)    
        record = self.add_264(row, record)
        if row['typ díla (celé dílo, úryvek, antologie, souborné dílo)'] == 'souborné dílo':
            self.add_994_book(row, df, record)     
        return record
    
    
    def create_record_part_of_book(self,row, df):
        """Creates record for part of the book.
        Adds all fields that are specific to parts of book"""
        record = Record(to_unicode=True,
            force_utf8=True)
        record.leader = '-----naa---------4i-4500'  
        ind = int(row['Je součást čeho (číslo záznamu)'])
        book_row = df.loc[df['Číslo záznamu'] == ind]
        # is the author same as in the collective work, or does the book has it's own author 
        if pd.isnull(row['Autor/ka + kód autority']):
            tup, record = self.add_author_code(book_row['Autor/ka + kód autority'].values[0], record)
            author = tup[0] 
            code = tup[1]
        else:
            tup, record = self.add_author_code(row['Autor/ka + kód autority'], record)
            author = tup[0] 
            code = tup[1]    
            
        if not pd.isnull(book_row['Další role'].values[0]):
            translators, record = self.get_translators_and_other_roles(book_row['Další role'].values[0], record) 
        else:
            translators = None  
            
        # from Dataframe to Pandas Series
        book_row = book_row.squeeze()
        record = self.add_008(book_row, record)
        record = self.add_264(book_row, record)
        record = self.add_commmon(row, record, author, code, translators)
        record = self.add_common_specific(row, record, author, translators)   
        record = self.add_994_part_of_book(row, record)
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
        
        record = self.add_008(row, record) 
        record = self.add_commmon(row, record, author, code, translators)
        record = self.add_common_specific(row, record, author, translators) 
        record = self.add_773(record, row)
        return record 

if __name__ == "__main__":
    
        # file with all czech translations and their id's 
    czech_translations="data/czech_translations_full_18_01_2022.mrc"
    # list of identifiers
    identifiers = []
    # dictionary with author - work:id pairs
    dict_author_work = {}

    # saves data from czech_translations file to dictionary dict_author_work  
    with open(czech_translations, 'rb') as data:
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
    # initial table 
    IN = 'data/preklady/Bibliografie_prekladu_fin.csv'
    # final file
    OUT = 'data/marc_fin.mrc'

    df = pd.read_csv(IN, encoding='utf_8')

    # table with authority codes
    finalauthority_path = 'data/finalauthority_simple.csv'
    finalauthority = pd.read_csv(finalauthority_path,  index_col=0)
    finalauthority.index= finalauthority['nkc_id']


        # writes data to file in variable OUT
    with open(OUT , 'wb') as writer:
        #iterates all rows in the table
        for index, row in df.iterrows():
            print(row['Číslo záznamu'])
            bib_fin = Bibliografie_record_fin(finalauthority = finalauthority, dict_author_work= dict_author_work, identifiers=identifiers, 
                                              fennica="data/fennica_czech.obj", clb_trl="data/clb_trl_fin.obj")
            title = row['Název díla dle titulu v latince']
            (author_name,author_code), _  = bib_fin.add_author_code(row['Autor/ka + kód autority'], Record(to_unicode=True,
                                                    force_utf8=True))

            if not (any(title == i for i in bib_fin.clb_trl['title_trl']) or any(author_code == i for i in bib_fin.clb_trl['author_code'])):
                if 'kniha' in row['Typ záznamu']: 
                    record = bib_fin.create_record_book(row, df)
                if 'část knihy' in row['Typ záznamu']: 
                    record = bib_fin.create_record_part_of_book(row, df)
                if 'článek v časopise' in row['Typ záznamu']:
                    record = bib_fin.create_article(row)
                print(record)    
                writer.write(record.as_marc())
    writer.close()
