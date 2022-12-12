import pandas as pd
import sys
sys.path.insert(1, 'C:/Users/Cezary/Documents/IBL-PAN-Python')
from my_functions import gsheet_to_df
from tqdm import tqdm
from ast import literal_eval

#%%

translations_df = pd.read_excel('translations_after_manual_full_geonames_2022-11-23.xlsx')
translations_df['geonames_id'] = translations_df['geonames_id'].apply(lambda x: literal_eval(x) if isinstance(x,str) else x)

geo_similarities = gsheet_to_df('1fuFy_ZemnkNTP6dAjc6Q3U3HJZL07vsduxDOER8A0ss', '20km')
geo_similarities = geo_similarities.loc[geo_similarities['valid'] == 'x'][['geonames_id', 'geonames_group']].values.tolist()
geo_similarities = dict(zip([int(e[0]) for e in geo_similarities], [int(e[-1]) for e in geo_similarities]))

duplicates = gsheet_to_df('1s4N_GiEXnWCFJa_0x5fZtCEwDjojtQxkc1a6KcTe4As', 'Arkusz2')
duplicates = duplicates.loc[duplicates['duplicates'] != 'not']
duplicates_list = set([int(e) for e in duplicates[1].to_list()])

test = translations_df.loc[translations_df['001'].isin(duplicates_list)]
test['geonames_group'] = test['geonames_id'].apply(lambda x: [el for el in [geo_similarities.get(e) for e in x] if el] if isinstance(x, list) else None)
test['geonames_group'] = test['geonames_group'].apply(lambda x: tuple(sorted(x)))

grouped = test.groupby(['author_id', 'work_id', 'year', 'language', 'geonames_group'])

df_grouped = pd.DataFrame()
for name, group in tqdm(grouped, total=len(grouped)):
    # name = ('106968086', 11556817.0, 1843, 'ger', (2879139,))
    # group = grouped.get_group(name)

    audience = '❦'.join([el for sub in [e.split('❦') for e in group['audience'].to_list()] for el in sub])
    fiction_type = '❦'.join([el for sub in [e.split('❦') for e in group['fiction_type'].to_list()] for el in sub])
    group_ids = '❦'.join([el for sub in [e.split('❦') for e in group['group_ids'].to_list()] for el in sub])
    
    one_from_group = group.head(1)
    one_from_group['audience'] = audience
    one_from_group['fiction_type'] = fiction_type
    one_from_group['group_ids'] = group_ids
    
    df_grouped = pd.concat([df_grouped, one_from_group])

translations_df = translations_df.loc[~translations_df['001'].isin(duplicates_list)]
translations_df = pd.concat([translations_df, df_grouped])

translations_df.to_excel('translations_final_database_2022_12_12.xlsx', index=False)
