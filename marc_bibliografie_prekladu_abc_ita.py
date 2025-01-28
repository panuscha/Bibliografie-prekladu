from marc_bibliografie_prekladu_abc import Bibliografie_record
from pymarc import Record, Subfield
import pandas as pd
from pymarc.field import Field
import re
import pickle
import marc_extra_codes

class Bibliografie_record_ita(Bibliografie_record): 
    
    def __init__(self, finalauthority_path, dict_author_work,dict_author__code_work_path):
        super(Bibliografie_record_ita, self).__init__(finalauthority_path, dict_author_work, dict_author__code_work_path)
        self.italian_articles =  ['il', 'lo', 'la', 'gli', 'le', 'i', 'un', 'una', 'uno', 'dei', 'degli', 'delle']
        self.tag = 'it22'
    
       
    def add_245(self,row, liability, title, subtitle, author, translators,  record):
        """Adds data to subfield 245. 
        Finds if work's title starts with an article -> writes how many positions the article takes
        """
                # matches first word in string
        first_word = re.search('^([\w]+)', title)
        if not first_word is None: 
            # matches first word in string
            first_word = re.search('^([\w]+)', title).group(0)
            if first_word.lower() in self.italian_articles:
                skip = str(len(first_word) + 1)
            else:
                skip = str(0)    
        else:
            skip = str(0)
        if title[0:2].lower() == "l'":
                skip = str(2)
        if title[0:2].lower() == "un'":
                skip = str(3) 
        c = self.c_245(row, liability, author, translators, 'traduzione di ')  
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
                
    def add_common_specific(self, row, record, author, translators):
        " 001, 240, title, subtitle, liability -> 245"
        if not(pd.isnull(row['Původní název'])) and not (("originál neznámý" in str(row['Původní název']).lower())  or ("originál neexistuje" in str(row['Původní název']).lower())):
            original_title = row['Původní název'].strip()                                                                       
            record.add_ordered_field(Field(tag='240', indicators = ['1', '0'], subfields = [Subfield(code='a', value= original_title),
                                                                                        Subfield(code='l', value= 'italsky'), ])) 
        if not(pd.isnull(row['Zdroj či odkaz'])) and not (row['Zdroj či odkaz'] == ' '):
            record.add_ordered_field(Field(tag = '998', indicators=[' ', ' '], subfields=[Subfield(code='a', value= row['Zdroj či odkaz'].strip()),] ) )    
            
        (title, subtitle) = self.get_title_subtitle(str(row['Název díla dle titulu (v příslušném písmu)']))
        liabiliy = row['Údaje o odpovědnosti a další informace']
        record = self.add_245(row, liabiliy, title, subtitle, author, translators,record) 

        rel_original = row['Vztah k originálu (překlad vs. adaptace)']
        if not(pd.isnull(rel_original)) and rel_original != 'překlad':    
            type_ad = row['Druh adaptace (slovem)']
            record = self.add_787(record, rel_original, type_ad if not(pd.isnull(type_ad)) else None )
            
        record.add_ordered_field(Field(tag = '500', indicators=[' ', ' '], subfields=[Subfield(code='a', value= "Záznam zpracován bez výtisku v ruce"), ]))
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
        translators = row['Překladatel/ka']

        if pd.isnull(row['Město vydání, země vydání, nakladatel']):publication_country = 'xx-'

        else:
            publication = row['Město vydání, země vydání, nakladatel'] 
            matches = re.findall(r'\(\s*(\w+)\s*\)', publication)

            if len(matches) == 1:
                country =  matches[0]
                if country == 'Itálie': publication_country = 'it-'
                elif country == 'Česká republika': publication_country = 'xr-'
                elif country : publication_country = 'xx-'         
            elif len(matches) == 2 : 
                if matches[0] == matches[1]: publication_country = 'it-'
                else:  publication_country = 'vp-'
            else: publication_country = 'xx-'    

        record = self.add_008(row, record, publication_country, 'mul' if ',' in row['Jazyk díla'] else 'ita')
        record = self.add_041(row, record)
        record = self.add_commmon(row, record, author, code, translators)  
        record = self.add_common_specific(row, record, author, translators)    
        record = self.add_264(row, record)

        if not pd.isnull(row['Edice, svazek']):    
            record = self.add_490(row['Edice, svazek'], record)

        #if row['typ díla (celé dílo, úryvek, antologie, souborné dílo)'] == 'souborné dílo':
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
        # from Dataframe to Pandas Series
        book_row = book_row.squeeze()
        # is the author same as in the collective work, or does the book has it's own author 
        if pd.isnull(row['Autor/ka + kód autority']):
            tup, record = self.add_author_code(book_row['Autor/ka + kód autority'], record)
            author = tup[0] 
            code = tup[1]
        else:
            tup, record = self.add_author_code(row['Autor/ka + kód autority'], record)
            author = tup[0] 
            code = tup[1]    
        translators = book_row['Překladatel/ka']

        if pd.isnull(book_row['Město vydání, země vydání, nakladatel']): publication_country = 'xx-'

        else:
            publication = book_row['Město vydání, země vydání, nakladatel'] 
            matches = re.findall(r'\(\s*(\w+)\s*\)', publication)

            if len(matches) == 1:
                country =  matches[0]
                if country == 'Itálie': publication_country = 'it-'
                elif country == 'Česká republika': publication_country = 'xr-'
                elif country : publication_country = 'xx-'         
            elif len(matches) == 2 : publication_country = 'vp-'
            else: publication_country = 'xx-' 
        
        record = self.add_008(book_row, record, publication_country, 'mul' if ',' in book_row['Jazyk díla'] else 'ita')
        record = self.add_041(book_row, record)
        record = self.add_264(book_row, record)
        if not pd.isnull(book_row['Edice, svazek']):    
            record = self.add_490(book_row['Edice, svazek'], record) 
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
        translators = row['Překladatel/ka']

        if pd.isnull(row['Město vydání, země vydání, nakladatel']):
            publication_country = 'xx-'

        else:
            publication = row['Město vydání, země vydání, nakladatel'] 
            start = publication.find('(')+1
            end = publication.find(')') 
            country = publication[start:end]
            if country == 'Itálie': publication_country = 'it-'
            elif country == 'Česká republika': publication_country = 'xr-'    
            else: publication_country = 'xx-'

        record = self.add_008(row, record, publication_country, "ita")
        record = self.add_041(row, record) 
        record = self.add_commmon(row, record, author, code, translators)
        record = self.add_common_specific(row, record, author, translators) 
        record = self.add_773(record, row)
        return record 

if __name__ == "__main__":
    
        # file with all czech translations and their id's 
    czech_translations="data/czech_translations_full_18_01_2022.mrc"

    dict_author_work_path = "data/dict_author_work.obj"

    dict_author__code_work_path =  "data/dict_author_code_work.obj"
    # initial table 
    IN = 'data/preklady/Bibliografie_prekladu_ita.csv'
    # final file
    OUT = 'data/marc_ita.mrc'

    df = marc_extra_codes.load_df_csv(IN, 'ita')

    err = [] 
    # table with authority codes
    finalauthority_path = 'data/finalauthority_simple.csv'

    # writes data to file in variable OUT
    with open(OUT , 'wb') as writer:
        #iterates all rows in the table
        for index, row in df.iterrows():
            try:            
                print(row['Číslo záznamu'])
                bib_ita = Bibliografie_record_ita(finalauthority_path = finalauthority_path, dict_author_work= dict_author_work_path, \
                                                    dict_author__code_work_path = dict_author__code_work_path)
                if 'kniha' in row['Typ záznamu']: 
                    record = bib_ita.create_record_book(row, df)
                if 'část knihy' in row['Typ záznamu']: 
                    record = bib_ita.create_record_part_of_book(row, df)
                if 'článek v časopise' in row['Typ záznamu']:
                    record = bib_ita.create_article(row)
                print(record)    
                writer.write(record.as_marc())
                pickle.dump( bib_ita.dict_author_work, open( "data/dict_author_work.obj", "wb" ) )
                pickle.dump( bib_ita.dict_author_code_work, open( "data/dict_author_code_work.obj", "wb" ) )
            except :
                err.append(row['Číslo záznamu'])
    print(err)            
    writer.close()

    