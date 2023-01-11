#%% import
import regex as re

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

# def parse_marc21_column

#%% example


marc21_field = '1\$aKundera, Milan,$d1929-$0(NL-LeOCL)06967521X$1http://viaf.org/viaf/51691735'
marc21_field_parsed = parse_marc21_field(marc21_field, '\\$')
print(marc21_field_parsed)


