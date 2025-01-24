from marc_bibliografie_prekladu_abc import Bibliografie_record
from pymarc import Record, Subfield
import pandas as pd
from pymarc.field import Field
import re
import pickle
import marc_extra_codes

class Bibliografie_record_fin(Bibliografie_record): 
    
    def __init__(self, finalauthority_path, dict_author_work, dict_author__code_work_path, fennica_path, clb_trl_path):
        super(Bibliografie_record_fin, self).__init__(finalauthority_path, dict_author_work, dict_author__code_work_path)
        self.tag = "fi24"
        self.df_fennica = pickle.load(open(fennica_path, "rb" ))
        self.clb_trl = pickle.load(open(clb_trl_path, "rb" ))
        
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
    

    def add_035(self, row, record ):
        "Adds FENNICA catalogue number to 035$a"
        if not pd.isnull(row["Finské id"]):
            finnish_id = row["Finské id"]
            fennica_info = self.df_fennica[self.df_fennica.index==finnish_id].squeeze()
                
            if fennica_info.empty:
                finnish_id = row["Finské id"]
                record.add_ordered_field(Field(tag='035', indicators = [' ', ' '], subfields = [Subfield(code='a', value= "(FENNICA)[{fennica}]".format(fennica = finnish_id))]))
        return record
    
    def add_998(self, row, record ):
        "Adds Melinda hypertext to 998$a"
        if not pd.isnull(row["Finské id"]):
            finnish_id = row["Finské id"]
            fennica_info = self.df_fennica[self.df_fennica.index==finnish_id].squeeze()
                
            if fennica_info.empty:
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
        if subtitle == '' and c == '' :                                                                           
            record.add_ordered_field(Field(tag = '245', indicators = ['0', skip], subfields = [Subfield(code='a', value= title + " ." ), ]))    # TODO - should this end with '/' ???     + " /"                                                                 
        else:
            if c == '':
                #subtitle = subtitle.strip()
                record.add_ordered_field(Field(tag = '245', indicators = ['0', skip], subfields = [Subfield(code='a', value= title + " :"), 
                                                                                        Subfield(code='b', value= subtitle + " ."),])) # TODO - should this end with '/' ???     + " /" 
            elif subtitle  == '':  
                c = c.strip()    
                record.add_ordered_field(Field(tag = '245', indicators = ['1', skip], subfields = [Subfield(code='a', value= title + " /"),
                                                                                                Subfield(code='c', value= c)]))
            else:
                c = c.strip() 
                record.add_ordered_field(Field(tag = '245', indicators = ['1', skip], subfields = [Subfield(code='a', value= title + " :"), 
                                                                                        Subfield(code='b', value= subtitle + " /"),
                                                                                        Subfield(code='c', value= c)]))
        return record        
                
    def add_common_specific(self, row, record, author, translators):
        " 001, 035, 240, title, subtitle, liability -> 245"
        record = self.add_035(row, record)
        
        if not(pd.isnull(row['Původní název'])) and not (("originál neznámý" in str(row['Původní název']).lower())  or ("originál neexistuje" in str(row['Původní název']).lower())):
            original_title = row['Původní název'].strip()                                                                       
            record.add_ordered_field(Field(tag='240', indicators = ['1', '0'], subfields = [Subfield(code='a', value= original_title),
                                                                                        Subfield(code='l', value= 'finsky'), ])) 
            
        title = row['Název díla dle titulu v latince'] if not pd.isnull(row['Název díla dle titulu v latince']) else ''
        subtitle  = row["Doplnění názvu" ]if not pd.isnull(row["Doplnění názvu"]) else ''
        liabiliy = row['Údaje o odpovědnosti a další informace (z titulní strany)']
        record = self.add_245(row, liabiliy, title, subtitle, author, translators,record) 
        record = self.add_998(row, record)
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

        if pd.isnull(row['Město vydání, země vydání, nakladatel']):
            publication_country = 'xx-'

        else:
            publication = row['Město vydání, země vydání, nakladatel'] 
            matches = re.findall(r'\(\s*(\w+)\s*\)', publication)

            if len(matches) == 1:
                country =  matches[0]
                if country == 'Finsko': publication_country = 'fi-'
                elif country == 'Česká republika': publication_country = 'xr-'
                elif country == 'Švédsko': publication_country = 'se-'
                else: publication_country = 'xx-'         
            elif len(matches) == 2 : 
                if matches[0] == matches[1]: publication_country = 'fi-'
                else:  publication_country = 'vp-'
            else: publication_country = 'xx-'      
          
        record = self.add_008(row, record, publication_country, "fin")
        record = self.add_041(row, record)
        record = self.add_commmon(row, record, author, code, translators)  
        record = self.add_common_specific(row, record, author, translators)    
        record = self.add_264(row, record)

        if not pd.isnull(row['Edice, svazek']):    
            record = self.add_490(row['Edice, svazek'], record)
        
        #if row['typ díla (celé dílo, úryvek, antologie, souborné dílo)'] == 'souborné dílo':
        self.add_994_book(row, df, record)   

        self.mine_fennica(record, row)
        return record
    
    
    def create_record_part_of_book(self,row, df):
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
            author = tup[0] 
            code = tup[1]
        else:
            tup, record = self.add_author_code(row['Autor/ka + kód autority'], record)
            author = tup[0] 
            code = tup[1]    
            
        if not pd.isnull(book_row['Další role']):
            translators, record = self.get_translators_and_other_roles(book_row['Další role'], record) 
        else:
            translators = None  

        if pd.isnull(book_row['Město vydání, země vydání, nakladatel']):
            publication_country = 'xx-'

        else:
            publication = book_row['Město vydání, země vydání, nakladatel'] 
            matches = re.findall(r'\(\s*(\w+)\s*\)', publication)

            if len(matches) == 1:
                country =  matches[0]
                if country == 'Finsko': publication_country = 'fi-'
                elif country == 'Česká republika': publication_country = 'xr-'
                elif country == 'Švédsko': publication_country = 'se-'
                else: publication_country = 'xx-'         
            elif len(matches) == 2 : 
                if matches[0] == matches[1]: publication_country = 'fi-'
                else:  publication_country = 'vp-'
            else: publication_country = 'xx-' 
            
             

            
        
        record = self.add_008(book_row, record, publication_country, 'mul' if ',' in book_row['Jazyk díla'] else 'fin')
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
        
        if not pd.isnull(row['Další role']):
            translators, record = self.get_translators_and_other_roles(row['Další role'] , record)  
        else:
            translators = None   

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
        
        record = self.add_008(row, record,publication_country, 'mul' if ',' in row['Jazyk díla'] else 'fin') 
        record = self.add_041(row, record)
        record = self.add_commmon(row, record, author, code, translators)
        record = self.add_common_specific(row, record, author, translators) 
        record = self.add_773(record, row)
        return record 
    
    def mine_fennica(self,record, row):
        if not pd.isnull(row["Finské id"]):
            finnish_id = row["Finské id"]
            fennica_info = self.df_fennica[self.df_fennica.index==finnish_id].squeeze()
            
            if not fennica_info.empty:    
                ### 008
                fennica_008 = fennica_info['008'][0]
                clb_008 = list(record['008'].data)
                for i in range(14, 38):
                    if fennica_008[i].isalnum() and not clb_008[i].isalnum():
                        clb_008[i] = fennica_008[i]
                record['008'].data = ''.join(clb_008)        
            print(record)
if __name__ == "__main__":
    
    # file with all czech translations and their id's 
    czech_translations="data/czech_translations_full_18_01_2022.mrc"

    # initial table 
    IN = 'data/preklady/Bibliografie_prekladu_fin.csv'
    # final file
    OUT = 'data/marc_fin.mrc'

    
    df = marc_extra_codes.load_df_csv(IN, 'fin')
    # table with authority codes
    finalauthority_path = 'data/finalauthority_simple.csv'


    dict_author_work_path = "data/dict_author_work.obj"
    dict_author__code_work_path = "data/dict_author_code_work.obj"
    
    duplications_finished = []
    duplications_unfinished = []
    dup_count_fin = 0
    dup_count_unfin = 0

    err = []
    found_records = []
        # writes data to file in variable OUT
    with open(OUT , 'wb') as writer:
        #iterates all rows in the table
        for index, row in df.iterrows():
            try:
                print(row['Číslo záznamu'])
                bib_fin = Bibliografie_record_fin(finalauthority_path = finalauthority_path, dict_author_work= dict_author_work_path, \
                                                    dict_author__code_work_path =  dict_author__code_work_path, \
                                                    fennica_path="data/fennica_czech.obj", clb_trl_path="data/clb_trl_fin.obj")
                title = row['Název díla dle titulu v latince']
                (author_name,author_code), _  = bib_fin.add_author_code(row['Autor/ka + kód autority'], Record(to_unicode=True,
                                                        force_utf8=True))
                
                year = row['Rok']
                print(year)

                if 'kniha' or 'antologie' in row['Typ záznamu']: 
                    record = bib_fin.create_record_book(row, df)
                if 'část knihy' in row['Typ záznamu']: 
                    record = bib_fin.create_record_part_of_book(row, df)
                if 'článek v časopise' in row['Typ záznamu']:
                    record = bib_fin.create_article(row)
                #print(record)    
                writer.write(record.as_marc())
                pickle.dump( bib_fin.dict_author_work, open( "data/dict_author_work.obj", "wb" ) )
                pickle.dump( bib_fin.dict_author_code_work, open( "data/dict_author_code_work.obj", "wb" ) ) 
            except :
                err.append(row['Číslo záznamu'])
    print(err)                 
    writer.close()
