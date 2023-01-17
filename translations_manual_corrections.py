import pandas as pd
import math
from ast import literal_eval

#%%

data_df = pd.read_excel('translations_clean_database.xlsx')
file_path = r"C:\Users\Cezary\Downloads\manual deduplication.xlsx"

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

#deduplication
def deduplication_phase(df, file_path):
    print('Deduplication phase started')
    deduplication_df = pd.read_excel(file_path, sheet_name='deduplication')
    deduplication_df['records ids'] = deduplication_df['records ids'].apply(lambda x: literal_eval(x))
    deduplication = list(zip(deduplication_df['records ids'], deduplication_df['proper record']))
    for duplicates, proper in deduplication:
        duplicates, proper = deduplication[0]
        for _ in duplicates:
            if int(_) not in [int(e) for e in df['001'].to_list()]:
                print(f"---> Record {_} is not in the dataframe")
        improper = [e for e in duplicates if e != proper]
        changes = {'fiction_type', 'audience'}
        
        for column in changes:
            extra_type = '❦'.join(df.loc[df['001'].isin(improper)][column].to_list())
            final_type = '❦'.join(df.loc[df['001'] == proper][column].to_list())
            final_type = final_type + '❦' + extra_type
            df.loc[df['001'] == proper, column] = final_type
            
        df = df.loc[~df['001'].isin(improper)]
    print('Deduplication phase completed')    
    return df

def manual_corrections(df, file_path):
    new_df = deletion_phase(df, file_path)
    new_df = reassignment_phase(new_df, file_path)
    new_df = deduplication_phase(new_df, file_path)
    return new_df

#%%main

new_df = manual_corrections(data_df, file_path)





















