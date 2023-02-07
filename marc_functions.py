#%% import
import pymarc
import io
import pandas as pd
import regex as re
import numpy as np
from tqdm import tqdm

#%% def
def mrc_to_mrk(path_in, path_out):
    reader = pymarc.MARCReader(open(path_in, 'rb'), to_unicode=True, force_utf8=True)
    writer = pymarc.TextWriter(io.open(path_out, 'wt', encoding="UTF-8"))
    for record in reader:
        writer.write(record)
    writer.close()
# The function converts .mrc binary file to .mrk text file. It uses pymarc MARCReader and pymarc TextTwriter to convert a file
    
def read_mrk(path):
    records = []
    with open(path, 'r', encoding='utf-8') as mrk:
        record_dict = {}
        for line in mrk.readlines():
            line = line.replace('\n', '')
            if line.startswith('=LDR'):
                if record_dict:
                    records.append(record_dict)
                    record_dict = {}
                record_dict[line[1:4]] = [line[6:]]
            elif line.startswith('='):
                key = line[1:4]
                if key in record_dict:
                    record_dict[key] += [line[6:]]
                else:
                    record_dict[key] = [line[6:]]
        records.append(record_dict)
    return records
# The function reads .mrk file as a list of strings. Each MARC21 field is represented as a separate list element. Then all of the fields of one bibliographic records are connected in dictionary. The output of the function is a list of dictionaries.

def mrk_to_df(mrk_variable):
    marc_df = pd.DataFrame(mrk_variable)
    fields = marc_df.columns.tolist()
    fields = [i for i in fields if 'LDR' in i or re.compile('\d{3}').findall(i)]
    marc_df = marc_df.loc[:, marc_df.columns.isin(fields)]
    fields.sort(key = lambda x: ([str,int].index(type("a" if re.findall(r'\w+', x)[0].isalpha() else 1)), x))
    marc_df = marc_df.reindex(columns=fields)  
    return marc_df
# The function converts list of dictionaries where each dictionary is a separate MARC21 record into pandas dataframe. The list of columns is sorted alphabetically except for the 'leader' which is located on the first position. Each cell represents individual MARC21 field including the indicators.

def df_to_mrc(df, path_out, txt_error_file_path):
    mrc_errors = []
    df = df.replace(r'^\s*$', np.nan, regex=True)
    outputfile = open(path_out, 'wb')
    errorfile = io.open(txt_error_file_path, 'wt', encoding='UTF-8')
    list_of_dicts = df.to_dict('records')
    for record in tqdm(list_of_dicts, total=len(list_of_dicts)):
        record = {k: v for k, v in record.items() if not isinstance(v, float)}
        try:
            pymarc_record = pymarc.Record(to_unicode=True, force_utf8=True, leader=record['LDR'][0].replace('\\', ' '))
            for k, v in record.items():
                if k == 'LDR':
                    pass
                elif k.isnumeric() and int(k) < 10:
                    tag = k
                    data = ''.join(v).replace('\\', ' ')
                    marc_field = pymarc.Field(tag=tag, data=data)
                    pymarc_record.add_ordered_field(marc_field)
                else:
                    if len(v) == 1:
                        tag = k
                        record_in_list = re.split('\$(.)', ''.join(v))
                        indicators = list(record_in_list[0])
                        subfields = record_in_list[1:]
                        marc_field = pymarc.Field(tag=tag, indicators=indicators, subfields=subfields)
                        pymarc_record.add_ordered_field(marc_field)
                    else:
                        for element in v:
                            tag = k
                            record_in_list = re.split('\$(.)', ''.join(element))
                            indicators = list(record_in_list[0])
                            subfields = record_in_list[1:]
                            marc_field = pymarc.Field(tag=tag, indicators=indicators, subfields=subfields)
                            pymarc_record.add_ordered_field(marc_field)
            outputfile.write(pymarc_record.as_marc())
        except ValueError as err:
            mrc_errors.append((err, record))
    if len(mrc_errors) > 0:
        for element in mrc_errors:
            errorfile.write(str(element) + '\n\n')
    errorfile.close()
    outputfile.close()
# The function converts pandas dataframe into MARC21 file and saves .mrc file on the local hard drive. The function's output is also a text file where all possible errors during the conversion are presented. The funcion converts a dataframe into list of dictionaries for the efficiency. Each dictionary is then saved as a pymarc record, where 'leader' field, control fields and bibliographic fields are properly parsed into MARC21 structure including indicators and subfields.

#%% main (example of use)

# #1. convert .marc or .mrc binary file to .mrk text file
# mrc_to_mrk('MARC21_file.marc', 'MARC21_file.mrk')
# #2. read .mrk text file
# mrk_file = read_mrk('MARC21_file.mrk')
# #3. convert .mrk file into dataframe
# mrk_df = mrk_to_df(mrk_file)
# #4. save dataframe as a .marc or .mrc binary file
# df_to_mrc(mrk_df, 'MARC21_file_new.marc', 'errors.txt')




















