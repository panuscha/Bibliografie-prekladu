from abc import ABC, abstractmethod 
from pymarc import Record, MARCReader, Subfield
import pandas as pd
from pymarc.field import Field
from datetime import datetime
import re
import random
import pickle
from collections import defaultdict


class Bibliografie_record(ABC): 

    def __init__(self, finalauthority_path, dict_author_work_path,dict_author_code_work_path,  **kwargs):
        self.finalauthority = pd.read_csv(finalauthority_path,  index_col=0)
        self.finalauthority.index= self.finalauthority['nkc_id']
        self.dict_author_work = pickle.load(open(dict_author_work_path, "rb" ))
        self.dict_author_code_work = pickle.load(open(dict_author_code_work_path, "rb" ))
        self.ordinal = ['první', 'druhé', 'třetí', 'čtvrté', 'páté','šesté', 'sedmé', 'osmé', 'deváté', 'desáté', 'jedenácté', 'dvanácté', 'třinácté', 'čtrnácté', 'patnácté', '1.', '2.', '3.', '4.', '5.', '6.']
    
    @abstractmethod
    def add_008(self,row, record, lang):
        """Creates fixed length data and adds them to field 008 """ 
        
    @abstractmethod
    def add_245(self, row, liability, title, subtitle, author, translators,  record):
        """Adds data to subfield 245. 
        Finds if work's title starts with an article -> writes how many positions the article takes
        """
        pass

    @abstractmethod   
    def add_common_specific(self, row, record, author,  translators): 
        " 001, 240, title, subtitle, liability -> 245"
        pass
    
    @abstractmethod   
    def add_994_book(self, row, df, record):
        """Adds id's of all parts of the collective work to field 994."""
        pass
        
    @abstractmethod    
    def create_record_part_of_book(self, row, df):
        """Creates record for part of the book.
    Adds all fields that are specific to parts of book"""
        pass
    
    @abstractmethod
    def create_record_book(self, row, df):
        """Creates record for book.
    Adds all fields that are specific to books"""
        pass
    
    @abstractmethod
    def create_article(self, row):
        """Creates record for the article.
    Adds all fields that are specific to articles."""
        pass
    


    def generate_id(self, code):
        """Generates 11 positional id for field 595. 
        In case there is no authority code, id is random.
        Otherwise generates id from the authority code.""" 
        ID_LENGTH = 11
        lowest = 10000
        highest = 99999
        if code is None:
            rand_number = str(random.randint(000000000000,999999999999))
            # in case random number has less than 11 positions, add zeros at the beginning
            if len(rand_number) < ID_LENGTH:
                for i in range(ID_LENGTH-len(rand_number)):
                    rand_number = "0" + rand_number
            return "ubc"+rand_number
        #rand_number = str(random.randint(1000,9999))
        rand_number = str(random.randint(lowest,highest))
        ret = "ubc"+code[0:4]+str(code[-2:])+"-"+rand_number
        # in case generated id already exists, create a new one
        rand_number = str(random.randint(lowest,highest))
        ret = "ubc"+code[0:4]+str(code[-2:])+"-"+rand_number    
        return ret  
    
    def add_041(self, row, record):
        
        work_lang = row['Jazyk díla']
        original_lang = row['Výchozí jazyk ']
        mediator = row['Zprostředkovací jazyk']
        
        if not pd.isnull(work_lang) and work_lang != 'nan': 
            #for w_l in work_lang.split(','):                                        
                #record.add_ordered_field(Field(tag='041', indicators=['1',' '],subfields=[Subfield(code='a', value= w_l.strip() )])) 
            record.add_ordered_field(Field(tag='041', indicators=['1',' '],subfields = [Subfield(code='a', value= w_l.strip() ) for w_l in work_lang.split(',')] )) 
        
        if not pd.isnull(original_lang):
            if original_lang == 'nan':
                original_lang = 'cze' 
            for o_l in original_lang.split(','):   
                record['041'].add_subfield('h', o_l.strip())
            
        if not pd.isnull(mediator):
            for m in mediator.split(','): 
                record['041'].add_subfield('k', m.strip())
        
        return record 

    def add_595(self,record, row, author, code):
        """Adds data to field 595. 
        Consists of author's name in subfield 'a', birth (and death) year in subfield 'd'
        author's code in subfield '7', original title of the work in subfield 't' and generated id in subfield 't'""" 
        original_work_title_orig = str(row['Původní název']).strip() if not pd.isnull(row['Původní název']) else '' # TODO: tohle nějak vyresit
        original_work_title = original_work_title_orig.lower()
        author_orig = author
        if author is not None : author = author.lower()  
        if pd.isnull(original_work_title) or ('originál nenalezen' in original_work_title) or ("originál neznámý" in original_work_title)  or ("originál neexistuje" in original_work_title):
            original_work_title = None                                                                      
        if author is not None:
            if not code is None:
                if code in self.finalauthority.index:
                        date = str(self.finalauthority.loc[code]['cz_dates' ]) 
                else:
                    date = None        
            else:
                date = None            
                id = None #self.generate_id(code) <-- we cannot associate id with code 
            
            if not code in self.dict_author_code_work.keys():
                if original_work_title is None:
                    id = None  
                elif not author in self.dict_author_work.keys():
                    id = self.generate_id(code)   
                    self.dict_author_work[author] = {original_work_title:id}   
                    self.dict_author_code_work[code] = {original_work_title:id}      
                else:
                    dict_titles = self.dict_author_work[author]
                    if original_work_title in dict_titles.keys():
                        id = dict_titles[original_work_title]
                    else:
                        id = self.generate_id(code) 
                        dict_titles[original_work_title] = id
                        self.dict_author_work[author] = dict_titles
                        self.dict_author_code_work[code] = {original_work_title:id}
            else:
                dict_titles = self.dict_author_code_work[code]
                if original_work_title is not None and original_work_title in dict_titles.keys():
                    id = dict_titles[original_work_title]
                else:
                    if original_work_title is None:
                        id = None  
                    else:
                        id = self.generate_id(code) 
                        dict_titles[original_work_title.lower()] = id
                        self.dict_author_code_work[code] = dict_titles                

            record.add_ordered_field(Field(tag='595', indicators = ['1', '2'], subfields = [Subfield(code='a', value=author_orig)  ]))
            if date is not None: 
                record['595'].add_subfield(code = 'd', value=date)
            if code is not None: 
                record['595'].add_subfield(code = '7', value=str(code))       
            if original_work_title_orig is not None and original_work_title_orig != '' and original_work_title_orig != 'nan':     
                record['595'].add_subfield(code = 't', value=original_work_title_orig)
            if id is not None:
                record['595'].add_subfield(code = '1', value=id.replace('-', ''))                   
            if not(pd.isnull(row['Údaje o zprostředkovacím díle'])):
                record['595'].add_subfield(code='i', value= row['Údaje o zprostředkovacím díle'].strip())
        elif not(pd.isnull(row['Údaje o zprostředkovacím díle'])):
                record.add_ordered_field(Field(tag='595', indicators = ['1', '2'], subfields = [Subfield(code='i', value=row['Údaje o zprostředkovacím díle'].strip())  ]))      #"Zdroj překladu: " +
        return record
    
    def add_008(self, row, record, publication_country, language):
        """Creates fixed length data and adds them to field 008 """ 
        date_record_creation = str(datetime.today().strftime('%y%m%d'))
        letter = 's'

        if pd.isnull(row['Rok']):
            publication_date = '--------'
        else:
            publication_date = str(row['Rok']).replace('.0','')+ '----' 

        material_specific =  '-----------------'
        modified = '-'
        cataloging_source = 'd'
        data = date_record_creation + letter + publication_date +  publication_country + material_specific + language + modified + cataloging_source
        record.add_ordered_field(Field(tag='008', indicators = [' ', ' '], data = data))
        return record


    def add_773(self, record, row):   
        """Adds data to field 773. Only for magazines.
        Consists of jurnal issue data. 
        In subfield 't' magazine's title, in subfield 'g' additional magazine data, in subfield '9' year of publication
        """ 
        # regex pattern to match a string before and after a comma
        pattern_comma = r"^(.*?),(.*)$"

        # Search for the pattern in the text
        comma = re.match(pattern_comma, str(row['Údaje o časopiseckém vydání']))
        if comma:
            magazine = comma.group(1).strip()  # Extract and strip leading/trailing whitespace
            rest = comma.group(2).strip() 
            
            subfields = [Subfield(code='t', value=magazine),
                        Subfield(code='g', value=rest)]
            year = row['Rok']
            if not pd.isnull(year): subfields += [Subfield(code='9', value= str(year).replace('.0',''))]
            record.add_ordered_field(Field(tag='773', indicators = ['0', ' '], subfields =  subfields))
        return record    
    
    def add_787(self, record, rel_to_original, adaptation = None):
        subfields = [Subfield(code = 'i', value = f'{rel_to_original} - {adaptation}' if adaptation is not None else f'{rel_to_original}' ),
                     Subfield(code = 'a', value = record['100']['a']),
                     Subfield(code = 't', value = record.title.rstrip(' /:'))]
        record.add_ordered_field(Field(tag='787', indicators = ['0', ' '], subfields=subfields))
        return record
                                                                        

    def add_author_code(self, data, record):
        """Adds authors name and code into field 100.
        Also returns author's name and code as a tuple.
        """ 
        if not(pd.isnull(data)):
            code_100 = True
            for data in data.split('§'): 
                start = data.find('(')
                end = data.find(')') 
                if start == -1:
                    author = data
                    code = None
                    record.add_ordered_field(Field(tag='100' if code_100 else '700', indicators=['1',' '], subfields=[Subfield(code='a', value=data.strip() ),
                                                                                            Subfield(code='4', value='aut')]))
                    #return (data, None), record
            
                else: 
                    Subfields = []
                    # matches everything before '(' character 
                    author = re.search('.*(?=\s+\()', data).group(0).strip()
                    code = data[start+1: end].strip()
                    Subfields.append(Subfield(code='a', value=author + ', '))  # comma manual page 42 
                    if code in self.finalauthority.index:
                            date = str(self.finalauthority.loc[code]['cz_dates']) 
                            Subfields.append(Subfield(code = 'd', value=date))
                    Subfields.append(Subfield(code = '7', value=code))
                    Subfields.append(Subfield(code = '4', value='aut'))        
                    record.add_ordered_field(Field(tag='100' if code_100 else '700', indicators=['1',' '], subfields=Subfields))  
                if code_100: author_ret = author
                if code_100: code_ret = code
                code_100 = False
            return (author_ret, code_ret), record
        else:
            return (None, None), record

    def get_title_subtitle(self,data):
        """Splits work's title into title and subtitle.
        Returns as tuple.
        """
        data = data.strip()
        split = data.find(':')
        if split == -1:
            return (data, '' )
        else:
            title = data[:split]
            subtitle = data[split+1:]
            title = title.strip()
            subtitle = subtitle.strip()      
            return(title, subtitle)  

    def add_264(self, row, record): ## TODO: WHAT TO DO WHEN 2 PUBLISHERS??
        """Adds data to subfield 264. 
        Consists of city of publication, coutry of publications and the publisher
        """
        if pd.isnull(row['Město vydání, země vydání, nakladatel']):
            return record    
        city_dict = {}
        year = row['Rok'] 
        city_country_publisher = row['Město vydání, země vydání, nakladatel'].strip()
        for element in city_country_publisher.split('§'):
                                    # Matches everything before ( 
            city_unknown = re.search('.*(?=\s+\()', element)
            if city_unknown:
                city_unknown = city_unknown.group(0)
                # if string contains ? -> city is unknown
                if  '?' in city_unknown:
                    city = "[s. l.]"
                else:  
                    # matches first words in string   
                    city =  re.search('^[\w\s]+', element).group(0).strip()
            else:
                city = "[s. l.]"        
            publisher = re.search('(?<=\:\s).+', element)
              

            subfield_publisher = []
 
            if publisher:
                publisher = publisher.group(0).strip()   
                subfield_publisher = [Subfield(code='a', value= city + ' : ') ]
                subfield_publisher += [Subfield(code='b', value=publisher + ', '), Subfield(code='c', value=str(year).replace('.0',''))]  if not pd.isnull(year) else [Subfield(code='b', value=publisher)]
                    
            else:      # no publisher is named
                subfield_publisher += [Subfield(code='a', value= city + ','), Subfield(code='c', value=str(year).replace('.0',''))]  if not pd.isnull(year) else [Subfield(code='a', value= city )]

   
            record.add_ordered_field(Field(tag = '264', indicators = [' ', '1'], subfields = subfield_publisher))
        return record            
    

    # def add_264(self, row, record): ## TODO: WHAT TO DO WHEN 2 PUBLISHERS??
    #     """Adds data to subfield 264. 
    #     Consists of city of publication, coutry of publications and the publisher
    #     """
    #     if pd.isnull(row['Město vydání, země vydání, nakladatel']):
    #         return record    
    #     city_dict = defaultdict(list)
    #     year = row['Rok'] 
    #     city_country_publisher = row['Město vydání, země vydání, nakladatel'].strip()
    #     for element in city_country_publisher.split('§'):
    #                                 # Matches everything before ( 
    #         city_unknown = re.search('.*(?=\s+\()', element)
    #         if city_unknown:
    #             city_unknown = city_unknown.group(0)
    #             # if string contains ? -> city is unknown
    #             if  '?' in city_unknown:
    #                 city = "[s. l.]"
    #             else:  
    #                 # matches first words in string   
    #                 city =  re.search('^[\w\s]+', element).group(0).strip()
    #         else:
    #             city = "[s. l.]"        
    #         publisher = re.search('(?<=\:\s).+', element)

            
    #         if publisher:
    #             publisher = publisher.group(0).strip() 
    #             if city == "[s. l.]" and len(city_dict.keys()) == 1 and city_dict.keys()[0] != city:
    #                 city_dict[city_dict.keys()[0]].append(publisher)
    #             else:
    #                 city_dict[city].append(publisher)
                
    #     subfield_publisher = []        
    #     for i,(city, publishers) in enumerate(city_dict.items()):
    #         if len(publishers) == 0:
    #             subfield_publisher += [Subfield(code='a', value= city + ','), Subfield(code='c', value=str(year).replace('.0',''))]  if not pd.isnull(year) else [Subfield(code='a', value= city )]
    #             record.add_ordered_field(Field(tag = '264', indicators = [' ', '1'], subfields = subfield_publisher))
    #             return 
    #         for j, publisher in enumerate(publishers) :
    #                 if i+1 < len(city_dict.keys()): add = ' ; '
    #                 elif j+1 < len(publishers): add = ' : '   
    #                 elif pd.isnull(year): add = '.'
    #                 else: add = ', '       
    #                 subfield_publisher = [Subfield(code='a', value= city + ' : '), Subfield(code='b', value=publisher + add)]

    #     if not pd.isnull(year) : subfield_publisher += Subfield(code='c', value=str(year).replace('.0',''))       
    #     record.add_ordered_field(Field(tag = '264', indicators = [' ', '1'], subfields = subfield_publisher))
        
    #     return record            


    def add_translator(self, translators, record):
        """Adds translators to field 700.
        In case there are more than 1 translator, multiplies field 700.
        """
        # iterates through all iteraters devided by character §
        for element in translators.split('§'):
                t = element.strip()
                if ',' not in t: 
                    t_names = t.split()
                    fist_name = ' '.join(t_names[:-1])
                    last_name = t_names[-1]
                    t = f'{last_name}, {fist_name}' 
                record.add_ordered_field(Field(tag='700', indicators=['1',' '], subfields=[Subfield(code='a', value= t),
                                                                                            Subfield(code='4', value= 'trl'),])) 
        return record        
                
    def c_245(self, row, liability,author, translators, liab_info):
        """Method for creating string used in field 245 subfield c.
        String structure: [Author's name] [Author's name]  ; liability info [translator's name] [translator's surname] ; liability informations 
        """
        c = ""
        #print("c 245 author: " + author)
        if not pd.isnull(author) and len(author) > 1: 
            surname = re.search('.*(?=,)', author )
            if surname:
                # finds the character "," a matches everything ahead it 
                surname = surname.group(0).strip() 
                # finds the character "," a matches everything behind it
                name = re.search('(?<=\,\s).+', author).group(0).strip()
                c += name + ' ' + surname + ' '
            else:
                c += author + ' '
                    
        if not(pd.isnull(translators)):  
            c += liab_info
            while True:
                    # there are more than 1 translator
                    if '§' in translators:
                        # finds character §
                        start = translators.find('§') 
                        surname = re.search('.*(?=,)',  translators[:start] )
                        if surname:      
                            # finds the character "," a matches everything ahead it 
                            surname = surname.group(0).strip() 
                            # finds the character "," a matches everything behind it 
                            name = re.search('(?<=\,\s).+', translators[:start]).group(0).strip() 
                            c += name + ' ' +  surname + ', '
                        else:
                            c += translators[:start] + ', '    
                        translators = translators[start + 1: ]
                    else:
                        break 
            surname = re.search('.*(?=,)',  translators )
            if surname:        
                # finds the character "," a matches everything ahead it 
                surname = surname.group(0).strip() 
                # finds the character "," a matches everything behind it 
                name = re.search('(?<=\,\s).+', translators).group(0).strip()
                c += name + ' ' +  surname 
            else:
                c += translators    
        if not pd.isnull(liability):
            c += ' ; ' + str(liability).strip()   
        return c  
    
    def add_490(self, value, record):#str(row['Edice, svazek'])
        # regex pattern to match a string before and after a comma
        pattern_comma = r"^(.*?);(.*)$"

        # Search for the pattern in the text
        comma = re.match(pattern_comma, value)
        
        if comma:
            series_statement = comma.group(1).strip()  # Extract and strip leading/trailing whitespace
            volume = comma.group(2).strip() 
            record.add_ordered_field(Field(tag='490', indicators = ['0', ' '], subfields = [Subfield(code = 'a', value=series_statement),
                                                                                            Subfield(code = 'v', value = volume) ]))
        else:
            record.add_ordered_field(Field(tag='490', indicators = ['0', ' '], subfields = [Subfield(code = 'a', value=value) ])) 
            
        return record   
      
    def add_994_book(self, row, df, record):
        """Adds id's of all parts of the collective work to field 994."""
        cislo_zaznamu = row['Číslo záznamu']
        is_part_of = df['Je součást čeho (číslo záznamu)']==cislo_zaznamu
        if any(is_part_of):
            book_rows = [i for i, val in enumerate(is_part_of) if val]
            for i in book_rows:
                r = df.iloc[i]
                number = self.tag+"".join(['0' for _ in range(6-len(str(row['Číslo záznamu'])))]) + str(r['Číslo záznamu'])
                record.add_ordered_field(Field(tag = '994', indicators = [' ', ' '], subfields = [Subfield(code= 'a', value = 'DN'),
                                                                                                Subfield(code = 'b',value = number)] ))   
        return record             
    
    def add_994_part_of_book(self, row, record):
        """Adds id of the collective work to field 994."""
        is_part_of = str(int(row['Je součást čeho (číslo záznamu)']))
        number = self.tag + "".join(['0' for a in range(6-len(is_part_of))]) + is_part_of
        record.add_ordered_field(Field(tag = '994', indicators = [' ', ' '], subfields = [Subfield(code= 'a', value = 'UP'),
                                                                                                Subfield(code = 'b',value = number)]))
        return record


    def ISBN(self, record, isbn_number): 
        isbn_number_original = isbn_number.replace('-', '')
        len_ISBN = len(isbn_number_original)
        if (len_ISBN == 10 or len_ISBN == 14) and  isbn_number_original.isnumeric(): 
            record.add_ordered_field(Field(tag='020', indicators=[' ',' '], subfields=[Subfield(code='a', value= isbn_number.strip()),] )) 
        return record
   
    def add_commmon(self, row, record, author, code, translators):
        """Adds data to fields that are common for all work types 
        """
        record.add_ordered_field(Field(tag='001', indicators = [' ', ' '], data=str(self.tag+ "".join(['0' for a in range(6-len(str(row['Číslo záznamu'])))]) + str(row['Číslo záznamu']))))
        record.add_ordered_field(Field(tag='003', indicators = [' ', ' '], data='CZ PrUCL')) 
        
        if not(pd.isnull(row['ISBN'])):
            record = self.ISBN(record, str(row['ISBN']))
            
        record.add_ordered_field(Field(tag='040', indicators=[' ',' '], subfields=[Subfield(code='a', value= 'ABB060'),
                                                                                Subfield(code='b', value= 'cze'),
                                                                                Subfield(code='e', value= 'rda'),]))
        
        if not(pd.isnull(row['Počet stran'])) : #and str(row['Počet stran']).isnumeric()
            record.add_ordered_field(Field(tag = '300', indicators=[' ', ' '], subfields=[Subfield(code='a', value= str(row['Počet stran']) + ' p.'), ]))

        if not(pd.isnull(row['Volná poznámka'])):
            for note in row['Volná poznámka'].split('§'): # greek is 
                if any(x in note for x in self.ordinal) and 'vydání' in note:
                    record.add_ordered_field(Field(tag = '250', indicators=[' ', ' '], subfields=[Subfield(code='a', value= note.strip() )]))
                elif 'dvojjazyčn' in note.lower():
                    record.add_ordered_field(Field(tag = '655', indicators=[' ', '7'], subfields=[Subfield(code='a', value= 'dvojjazyčná vydání' ),
                                                                                                  Subfield(code='7', value= 'fd194533' ),
                                                                                                  Subfield(code='2', value= 'czenas' )]))
                else:     
                    record.add_ordered_field(Field(tag = '500', indicators=[' ', ' '], subfields=[Subfield(code='a', value= note.strip()) ]))

        if not(pd.isnull(row['technická poznámka'])):   
            record.add_ordered_field(Field(tag = '500', indicators=[' ', ' '], subfields=[Subfield(code='a', value= row['technická poznámka']) ]))     
        
        record = self.add_595(record, row, author, code)  
        

        if not(pd.isnull(translators)):
            self.add_translator(translators, record ) 

        record.add_ordered_field(Field(tag = '910', indicators=[' ', ' '], subfields=[Subfield(code='a', value= 'ABB060'), ] ) )
        record.add_ordered_field(Field(tag = '964', indicators=[' ', ' '], subfields=[Subfield(code='a', value= 'TRL'),] ) )
        record.add_ordered_field(Field(tag = 'OWN', indicators = [' ', ' '], subfields = [Subfield(code='a', value= 'UCLA'),]))
        return record
    



    


    