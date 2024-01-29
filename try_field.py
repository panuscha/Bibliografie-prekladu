from pymarc import Record, MARCReader, Subfield
import pandas as pd
from pymarc.field import Field
from datetime import datetime
import re
import random

record = Record(to_unicode=True,
        force_utf8=True)

record.add_ordered_field(Field(tag='100', indicators=['1',' '], subfields=[Subfield(code='a', value='aaaaa'),
                                                                                       Subfield(code='4', value='aut')]))
record['100'].add_subfield(code = 'c', value= 'ccccc')

string= 'asdasf§sadasf§as'

print(string.split('x'))