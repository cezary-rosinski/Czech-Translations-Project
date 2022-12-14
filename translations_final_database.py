import pandas as pd
import sys
sys.path.insert(1, 'C:/Users/Cezary/Documents/IBL-PAN-Python')
from my_functions import gsheet_to_df, marc_parser_dict_for_field
from tqdm import tqdm
from ast import literal_eval
import regex as re

#%%

viaf_correction_dict = {'297343866': '15182712',
                        '109465704': '49250791',
                        '7685151778235218130002': '66469255',
                        '311313837': '9432935'}

work_correction_dict = {10000018276: 52467785,
                        10000018275: 54780412,
                        10000014610: 62705329,
                        10000006512: 10000010198,
                        42113759: 42131545,
                        10000015487: 185840237,
                        632746483: 12631822,
                        10000001684: 15316465}

translations_df = pd.read_excel('translations_after_manual_full_geonames_2022-11-23.xlsx')

translations_df['author_id'] = translations_df['author_id'].apply(lambda x: viaf_correction_dict.get(x,x))
translations_df['work_id'] = translations_df['work_id'].apply(lambda x: work_correction_dict.get(x,x))
translations_df['geonames_id'] = translations_df['geonames_id'].apply(lambda x: literal_eval(x) if isinstance(x,str) else x)

duplicates1 = gsheet_to_df('1s4N_GiEXnWCFJa_0x5fZtCEwDjojtQxkc1a6KcTe4As', 'current')
duplicates1 = duplicates1.loc[duplicates1['duplicates'] != 'not'][['group_number', 1]].groupby('group_number')

duplicates2 = gsheet_to_df('1s4N_GiEXnWCFJa_0x5fZtCEwDjojtQxkc1a6KcTe4As', 'duplicates for duplicated people')
duplicates2 = duplicates2.loc[duplicates2['duplicates'] == 'x'][['group_number', 1]].groupby('group_number')

duplicates_list = [duplicates1, duplicates2]

duplicates_ids = []
df_grouped = pd.DataFrame()
for duplicates in tqdm(duplicates_list):
    
    duplicates_dict = {}
    
    for name, group in duplicates:
        duplicates_ids.append([int(e) for e in group[1].to_list()])
        duplicates_dict[name] = [int(e) for e in group[1].to_list()]
    
    for k,v in tqdm(duplicates_dict.items()):
        temp_df = translations_df.loc[translations_df['001'].isin(v)]
        audience = '❦'.join([el for sub in [e.split('❦') for e in temp_df['audience'].to_list()] for el in sub])
        fiction_type = '❦'.join([el for sub in [e.split('❦') for e in temp_df['fiction_type'].to_list()] for el in sub])
        group_ids = '❦'.join([el for sub in [e.split('❦') for e in temp_df['group_ids'].to_list()] for el in sub])
        
        one_from_group = temp_df.sort_values('001').head(1)
        one_from_group['audience'] = audience
        one_from_group['fiction_type'] = fiction_type
        one_from_group['group_ids'] = group_ids
        
        df_grouped = pd.concat([df_grouped, one_from_group])

duplicates_ids = [e for sub in duplicates_ids for e in sub]

translations_df = translations_df.loc[~translations_df['001'].isin(duplicates_ids)]
translations_df = pd.concat([translations_df, df_grouped])
#CLEAN file
translations_df.to_excel('translations_clean_database.xlsx', index=False)

#CLEAN exploded

stage_zero = pd.read_excel('initial_stage_2022-03-07.xlsx')
records_ids = [ele for sub in [[int(el) for el in e.split('❦')] for e in translations_df['group_ids'].to_list()] for ele in sub]
clean_df_exploded = stage_zero.loc[stage_zero['001'].isin(records_ids)]
clean_df_exploded.to_excel('translations_clean_database_extended.xlsx', index=False)

#%% authority file

viaf_used = set(translations_df['author_id'].to_list()) #1211
authority_file = gsheet_to_df('1yKcilB7SEVUkcSmqiTPauPYtALciDKatMvgqiRoEBso', 'Sheet1')

#%% ABCD subsets

#A meta-records -- 10639 records from 33997 records

group_A = translations_df.loc[(translations_df['group_ids'].str.contains('❦', regex=False)) &
                              (translations_df['work_id'].notnull())]
# records_ids = [ele for sub in [[int(el) for el in e.split('❦')] for e in group_A['group_ids'].to_list()] for ele in sub]

#B individual records -- 10755 records

group_B = translations_df.loc[(~translations_df['group_ids'].str.contains('❦', regex=False)) &
                              (translations_df['work_id'].notnull())]

#C poor records -- 4607 records from 4616 records
group_AB_ids = pd.concat([group_A, group_B])['001'].to_list()

group_C = translations_df.loc[~translations_df['001'].isin(group_AB_ids)] #4616
group_C = group_C.groupby(['author_id', 'year', 'language']).head(1)

#D difficult records -- 1080 records from 1454 records (with not valid, e.g. wrong VIAFs: from 1536)
difficult_sheets = ['Multiple original titles', 'Selections', 'No Czech original', 'CLT antolo']

group_D = pd.concat([gsheet_to_df('1fzLeRaQnayWXlYeU5XCJWkSDhf4CeznM3NAyM9YLE00', e) for e in difficult_sheets])
group_D['author_id'] = group_D['100_unidecode'].apply(lambda x: [e.get('$1') for e in marc_parser_dict_for_field(x, '\$') if '$1' in e])
group_D['author_id'] = group_D['author_id'].apply(lambda x: re.findall('\d+', x[0])[0] if x else None)

viafreplacedict = {
    '256578118': '118529174',
    '78000938': '163185334',
    '10883385': '88045299',
    '83955898': '25095273',
    '2299152636076120051534': '11196637',
    '88045299|10883385': '88045299', 
    '118529174|256578118': '118529174',
    '163185334|78000938': '163185334',
    '25095273|83955898': '25095273',
    '11196637|2299152636076120051534': '11196637',
    '109465704': '49250791',
    '297343866': '15182712',
    '311313837': '9432935',
    '7685151778235218130002': '66469255',
}

group_D['author_id'] = group_D['author_id'].apply(lambda x: viafreplacedict.get(x,x))

authority_file = gsheet_to_df('1yKcilB7SEVUkcSmqiTPauPYtALciDKatMvgqiRoEBso', 'Sheet1')
authority_file_viafs = authority_file.loc[authority_file['used in clean file'] == 'True']['proper_viaf_id'].to_list()

group_D = group_D.loc[group_D['author_id'].isin(authority_file_viafs)]

def get_year(x):
    try:
        return int(x[7:11])
    except ValueError:
        return int(x[7:11].replace('u','0'))
        
group_D['year'] = group_D['008'].apply(lambda x: get_year(x))

group_D = group_D.groupby(['author_id', 'year', 'language']).head(1)

writer = pd.ExcelWriter('translations_ABCD_subgroups.xlsx', engine = 'xlsxwriter')

group_dict = {'group A': group_A,
              'group B': group_B,
              'group_C': group_C,
              'group_D': group_D}

[group_dict.get(e).to_excel(writer, index=False, sheet_name=e) for e in group_dict]
writer.save()
writer.close()
























#%% notes
# translations_df = pd.read_excel('translations_final_database_2022_12_12.xlsx')
# translations_df['geonames_id'] = translations_df['geonames_id'].apply(lambda x: literal_eval(x) if isinstance(x,str) else x)
# #na podstawie tych par określić kolejne duplikaty
# viafreplacedict = {
#     '256578118' : '118529174',
#     '78000938' : '163185334',
#     '10883385' : '88045299',
#     '83955898' : '25095273',
#     '2299152636076120051534' : '11196637',
#     '88045299|10883385' : '88045299', 
#     '118529174|256578118' : '118529174',
#     '163185334|78000938' : '163185334',
#     '25095273|83955898' : '25095273',
#     '11196637|2299152636076120051534' : '11196637',
#     '49250791' : '109465704',
#     '15182712' : '297343866',
#     '9432935' : '311313837',
#     '66469255' : '7685151778235218130002',
# }

# test = translations_df.loc[translations_df['author_id'].isin(viafreplacedict)]['author_id']

# test_set = set(test)

# '15182712', '49250791', '66469255', '83955898', '9432935'

# x = ['15182712', '297343866'] #Frais, Josef
# test_x = translations_df.loc[translations_df['author_id'].isin(x)]

# y = ['49250791', '109465704'] #Volkova, Bronislava -- tu są duplikaty
# test_y = translations_df.loc[translations_df['author_id'].isin(y)]

# z = ['66469255', '7685151778235218130002']
# test_z = translations_df.loc[translations_df['author_id'].isin(z)]

# a = ['9432935', '311313837']
# test_a = translations_df.loc[translations_df['author_id'].isin(a)]

# test_merged = pd.concat([test_x, test_y, test_z, test_a])
# test_merged['author_id'] = test_merged['author_id'].apply(lambda x: viafreplacedict.get(x, x))
# set(test_merged['author_id'])
# test_merged['geonames_group'] = test_merged['geonames_id'].apply(lambda x: [el for el in [geo_similarities.get(e,e) for e in x] if el] if isinstance(x, list) else None)

# ttt = test_merged.copy()[['001', '100', 'author_id', '245', 'work_id', '260', 'geonames_id', 'geonames_name', 'geonames_group', 'year', 'language']]
# ttt = ttt.sort_values(['author_id', 'language', 'year', 'work_id'])

# ttt = ttt.loc[(ttt['geonames_id'].notnull()) &
#                (ttt['geonames_group'].notnull())]

# ttt['geonames_group'] = ttt['geonames_group'].apply(lambda x: tuple(sorted(x)))

# # ttt = ttt.explode('geonames_group')

# grouped = ttt.groupby(['author_id', 'year', 'language'])

# df_grouped = pd.DataFrame()
# group_number = 1
# for name, group in tqdm(grouped, total=len(grouped)):
#     if group.shape[0] > 1:
#         group['group_number'] = group_number
#         group_number += 1
#         df_grouped = pd.concat([df_grouped, group])

# df_grouped = df_grouped.sort_values(['author_id', 'work_id', 'language', 'year'])





















