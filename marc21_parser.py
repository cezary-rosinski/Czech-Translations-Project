#%% import
import regex as re
import pandas as pd
from itertools import chain


#%% def
def parse_marc21_field(string, subfield_code):
    subfield_list = re.findall(f'{subfield_code}.', string)
    for subfield in subfield_list:
        subfield_escape = re.escape(subfield)
        string = re.sub(f'({subfield_escape})', r'❦\1', string)
    string = [e.split('\n')[0].strip() for e in string.split('❦') if e]
    list_of_dictionaries = [e for e in string if re.escape(e)[:len(subfield_code)] == subfield_code]
    list_of_dictionaries = [{subfield_list[i]:e[len(subfield_list[i]):]} for i, e in enumerate(list_of_dictionaries)]
    return list_of_dictionaries
# The function parses a MARC21 record stored as a string and converts it to a dictionary list. The target form is not in dictionary form, as individual fields in the MARC21 format may have repeated subfields. The function requires a string and a special character to indicate the appearance of a subfield.

def parse_marc21_column(df, field_id, field_data, subfield_code, delimiter='❦'):
    marc_field = df.loc[df[field_data].notnull(),[field_id, field_data]]
    marc_field = pd.DataFrame(marc_field[field_data].str.split(delimiter).tolist(), marc_field[field_id]).stack()
    marc_field = marc_field.reset_index()[[0, field_id]]
    marc_field.columns = [field_data, field_id]
    subfield_list = df[field_data].str.findall(f'{subfield_code}.').dropna().tolist()
    if marc_field[field_data][0].find(subfield_code[-1]) == 0: 
        subfield_list = sorted(set(list(chain.from_iterable(subfield_list))))
        subfield_list = [x for x in subfield_list if re.findall(f'{subfield_code}\w+', x)]
        empty_table = pd.DataFrame(index = range(0, len(marc_field)), columns = subfield_list)
        marc_field = pd.concat([marc_field.reset_index(drop=True), empty_table], axis=1)
        for marker in subfield_list:
            marker = "".join([i if i.isalnum() else f'\\{i}' for i in marker])            
            marc_field[field_data] = marc_field[field_data].str.replace(f'({marker})', r'❦\1', 1)
        for marker in subfield_list:
            marker2 = "".join([i if i.isalnum() else f'\\{i}' for i in marker])
            string = f'(^)(.*?\❦{marker2}|)(.*?)(\,{{0,1}})((\❦{subfield_code})(.*)|$)'
            marc_field[marker] = marc_field[field_data].str.replace(string, r'\3')
            marc_field[marker] = marc_field[marker].str.replace(marker, '').str.strip().str.replace(' +', ' ')
    else:
        subfield_list = list(set(list(chain.from_iterable(subfield_list))))
        subfield_list = [x for x in subfield_list if re.findall(f'{subfield_code}\w+', x)]
        subfield_list.sort(key = lambda x: ([str,int].index(type("a" if re.findall(r'\w+', x)[0].isalpha() else 1)), x))
        empty_table = pd.DataFrame(index = range(0, len(marc_field)), columns = subfield_list)
        marc_field['indicator'] = marc_field[field_data].str.replace(f'(^.*?)({subfield_code}.*)', r'\1')
        marc_field = pd.concat([marc_field.reset_index(drop=True), empty_table], axis=1)
        for marker in subfield_list:
            marker = "".join([i if i.isalnum() else f'\\{i}' for i in marker])            
            marc_field[field_data] = marc_field[field_data].str.replace(f'({marker})', r'❦\1', 1)
        for marker in subfield_list:
            marker2 = "".join([i if i.isalnum() else f'\\{i}' for i in marker]) 
            string = f'(^)(.*?\❦{marker2}|)(.*?)(\,{{0,1}})((\❦{subfield_code})(.*)|$)'
            marc_field[marker] = marc_field[field_data].apply(lambda x: re.sub(string, r'\3', x) if marker in x else '')
            marc_field[marker] = marc_field[marker].str.replace(marker, '').str.strip().str.replace(' +', ' ')
    for (column_name, column_data) in marc_field.iteritems():
        if re.findall(f'{subfield_code}', str(column_name)):
            marc_field[column_name] = marc_field[column_name].str.replace(re.escape(column_name), '❦')
    return marc_field
# The function parses a defined MARC21 field (available as pd.Series), which is accessible as a dataframe. The result of the function is a new dataframe that places the individual subfields in the appropriate columns. The header of each column refers to the subfields stored in pd.Series of the defined MARC21 field. The function takes the following arguments: dataframe, field with unique identifier, field to be parsed, special character to indicate the appearance of a subfield, special character in the role of delimiter between each usage of a field.

#%% example

# #parsing field
# marc21_field = '1\$aKundera, Milan,$d1929-$0(NL-LeOCL)06967521X$1http://viaf.org/viaf/51691735'
# marc21_field_parsed = parse_marc21_field(marc21_field, '\\$')
# print(marc21_field_parsed)
# #parsing dataframe

# marc21_data = {'001': [1, 2, 3, 4, 5],
#                '100': ['1\$aKundera, Milan,$d1929-$0(NL-LeOCL)06967521X$1http://viaf.org/viaf/51691735',
#                      '1\$aKundera, Milan,$d1929-,$eauthor.$1http://viaf.org/viaf/51691735',
#                      '1\$aKundera, Milan$d(1929-....).$4aut$1http://viaf.org/viaf/51691735',
#                      '1\$aKundera, Milan,$d1929-$1http://viaf.org/viaf/51691735',
#                      '1\$aKundera, Milan,$d 1929-$7jk01070894$4aut$1http://viaf.org/viaf/51691735'],
#              '245': ['13$aal-Buṭʼ :$briwāyah /$cMīlān Kūndīrā ; tarjamat Munīrah muṣṭafá.',
#                      '13$aal-Ḥayāt fī makān ʼāk̲ar /$cMīlān Kūndīrā ; tarjamat Muḥammad al-Tuhāmī al-ʻAmmārī.',
#                      '10$aAmodio barregarriak /$cMilan Kundera ; itzultzailea, Karlos Cid Abasolo.$1http://viaf.org/viaf/275194741',
#                      '10$aIzatearen arintasun jasanezina$h[Texto impreso]$cMilan Kundera; Karlos Cid Abasolo, itzultzailea.',
#                      '10$aBesmrtnost /$cM. Kundera ; s češkog prevela Sanja Milićević.$1http://viaf.org/viaf/214778525']}

# df = pd.DataFrame(marc21_data)
# parsed_100 = parse_marc21_column(df, '001', '100', '\\$', delimiter='❦')







































