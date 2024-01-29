from pymarc import MARCReader

#my_marc_file = "data/marc_it.mrc"
my_marc_file= 'data/fennica.mrcx'#"data/czech_translations_full_18_01_2022.mrc"

with open(my_marc_file, 'rb') as data:
    reader = MARCReader(data, to_unicode=True, force_utf8=True, utf8_handling="strict")

    for record in reader:
        if record is None:
            print(
                "Current chunk: ",
                reader.current_chunk,
                " was ignored because the following exception raised: ",
                reader.current_exception
            )
        else:
            print(record)
            # #print (record['595'] )
            # if not record['595'] is None:
            #     work = record['595']['t']
            #     if not work is None: 
            #         if work[-1] == '.':
            #             work = work[0:len(work)-1]
            #         print(work, " : ", record['595']['1'])

           