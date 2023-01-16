import pandas as pd
import math
from ast import literal_eval



data_df = pd.read_excel('translations_clean_database.xlsx')
test = data_df.copy()
ttt = pd.read_excel(r"C:\Users\Cezary\Downloads\manual deduplication.xlsx", sheet_name='reassignment')

#deletion
def deletion_phase(df, file_path):
    print('Deletion phase started')
    deletion_df = pd.read_excel(file_path, sheet_name='deletion')
    deletion_ids = set(deletion_df['record id'].to_list())
    for _ in deletion_ids:
        if int(_) not in df['001'].to_list():
            print(f"---> Record {_} is not in the dataframe")
    df = df.loc[~df['001'].isin(deletion_ids)]
    print('Deletion phase completed')
    return df
#reassignment
def reassignment_phase(df, file_path):
    # df = new_data_df.copy()
    # file_path = r"C:\Users\Cezary\Downloads\manual deduplication.xlsx"
    print('Reassignment phase started')
    reassignment_df = pd.read_excel(file_path, sheet_name='reassignment')
    reassignment_set = {'author_id', 'work_id', 'language', 'fiction_type', 'audience'}
    
    for reas in reassignment_set:
        # print(reas)
        if reas in ['author_id', 'work_id']:
            replacement = {k:v for k,v in dict(zip(reassignment_df['record id'], reassignment_df[reas])).items() if not math.isnan(v)}
        else:
            replacement = {k:v for k,v in dict(zip(reassignment_df['record id'], reassignment_df[reas])).items() if isinstance(v, str)} 
        for _ in replacement:
            if int(_) not in [int(e) for e in df['001'].to_list()]:
                print(f"---> Record {_} is not in the dataframe")
        for k,v in replacement.items():
            if not isinstance(v, float) and v.isalpha():
                df.loc[df['001'] == int(k), reas] = v    
            else: df.loc[df['001'] == int(k), reas] = int(v)
    print('Reassignment phase completed')
    return df

new_data_df = deletion_phase(data_df, r"C:\Users\Cezary\Downloads\manual deduplication.xlsx")
new_data_df = reassignment_phase(new_data_df, r"C:\Users\Cezary\Downloads\manual deduplication.xlsx")
   
data_df.loc[data_df['001'] == 1175975131]['author_id']
new_data_df.loc[new_data_df['001'] == 1175975131]['author_id']

data_df.loc[data_df['001'] == 65993830]['language']
new_data_df.loc[new_data_df['001'] == 65993830]['language']

data_df.loc[data_df['001'] == 1175975131]['fiction_type']
new_data_df.loc[new_data_df['001'] == 1175975131]['fiction_type']

data_df.loc[data_df['001'] == 1175975131]['audience']
new_data_df.loc[new_data_df['001'] == 1175975131]['audience']


#deduplication
print('Deduplication phase')
deduplication_df = pd.read_excel(r"C:\Users\Cezary\Downloads\manual deduplication.xlsx", sheet_name='deduplication')
deduplication_df['records ids'] = deduplication_df['records ids'].apply(lambda x: literal_eval(x))
deduplication_df = deduplication_df.explode('records ids')

deduplication_grouped = deduplication_df.groupby('proper record')


for name, group in deduplication_grouped:
    name = 237197114
    group = deduplication_grouped.get_group(name)
    group_ids = set(group['records ids'].to_list())
    for _ in group_ids:
        if int(_) not in data_df['001'].to_list():
            print(f"---> Record {_} is not in the dataframe")
            
            
    for column in group:
        if column in ['fiction_type', 'audience', '490', '500', '650', '655', '700']:
            sub_group[column] = '❦'.join(sub_group[column].dropna().drop_duplicates().astype(str))
        else:
            try:
                sub_group[column] = max(sub_group[column].dropna().astype(str), key=len)
            except ValueError:
                sub_group[column] = np.nan
    df_oclc_deduplicated = df_oclc_deduplicated.append(sub_group)
    
    
[e for e in data_df['001'].to_list() if e == 1187030940]

data_df.loc[data_df['001'] == 4792338]['author_id']
new_data_df.loc[new_data_df['001'] == 4792338]['author_id']


data_df.columns.values






















