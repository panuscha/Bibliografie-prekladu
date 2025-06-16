from pymarc import XMLWriter, MARCReader



with open('data/marc_bibliografie_prekladu_a_d_add_trl_3.mrc' , 'rb') as data:
    reader = MARCReader(data, to_unicode=True, force_utf8=True, utf8_handling="strict")
    # writing to a file
    writer = XMLWriter(open('data/marc_bibliografie_prekladu_a_d_opraveno_final.xml','wb'))
    for record in reader:
        writer.write(record)
    writer.close() 