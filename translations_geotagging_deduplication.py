import pandas as pd
from ast import literal_eval
import sys
sys.path.insert(1, 'C:/Users/Cezary/Documents/IBL-PAN-Python')
from my_functions import marc_parser_dict_for_field, gsheet_to_df
from tqdm import tqdm

#%%

def get_publishing_house(x):
    try:
        return [e.get('$b') for e in marc_parser_dict_for_field(x, '\\$') if '$b' in e][0]
    except IndexError:
        return None

#%%

translations_df = pd.read_excel('translations_after_manual_full_geonames_2022-11-23.xlsx')
translations_df['geonames_id'] = translations_df['geonames_id'].apply(lambda x: literal_eval(x) if isinstance(x,str) else x)
translations_df['geonames_name'] = translations_df['geonames_name'].apply(lambda x: literal_eval(x) if isinstance(x,str) else x)

warsaw_group_ids = [3012649, 2968529, 2980916, 3012621]
warsaw_group_names = ['Warsaw', 'Ożarów Mazowiecki', 'Pruszków', 'Izabelin']

warsaw_group = translations_df.loc[translations_df['geonames_name'].apply(lambda x: True if not isinstance(x, float) and [e for e in x if e in warsaw_group_names]  else False)][['001', '100', 'author_id', '245', 'work_id', '260', 'geonames_id', 'geonames_name', 'year']]
warsaw_group['publishing house'] = warsaw_group['260'].apply(lambda x: get_publishing_house(x))
warsaw_group.to_excel('test.xlsx', index=False)

hamburg_group_names = ['Reinbek', 'Hamburg', 'Hamburg-Nord']
hamburg_group = translations_df.loc[translations_df['geonames_name'].apply(lambda x: True if not isinstance(x, float) and [e for e in x if e in hamburg_group_names]  else False)][['001', '100', 'author_id', '245', 'work_id', '260', 'geonames_id', 'geonames_name', 'year']]
hamburg_group['publishing house'] = hamburg_group['260'].apply(lambda x: get_publishing_house(x))

#%% geo deduplication

translations_df = pd.read_excel('translations_after_manual_full_geonames_2022-11-23.xlsx')
translations_df['geonames_id'] = translations_df['geonames_id'].apply(lambda x: literal_eval(x) if isinstance(x,str) else x)

geo_similarities = gsheet_to_df('1fuFy_ZemnkNTP6dAjc6Q3U3HJZL07vsduxDOER8A0ss', '20km')
geo_similarities = geo_similarities.loc[geo_similarities['valid'] == 'x'][['geonames_id', 'geonames_group']].values.tolist()
geo_similarities = dict(zip([int(e[0]) for e in geo_similarities], [int(e[-1]) for e in geo_similarities]))

test = translations_df.copy()
test['geonames_group'] = test['geonames_id'].apply(lambda x: [el for el in [geo_similarities.get(e) for e in x] if el] if isinstance(x, list) else None)

ttt = test.copy()[['001', '100', 'author_id', '245', 'work_id', '260', 'geonames_id', 'geonames_name', 'geonames_group', 'year', 'language']]
ttt = ttt.loc[(ttt['geonames_id'].notnull()) &
              (ttt['work_id'].notnull())
              [ttt['geonames_group'].apply(lambda x: True if x else False)]]
ttt['geonames_group'] = ttt['geonames_group'].apply(lambda x: tuple(sorted(x)))

# ttt = ttt.explode('geonames_group')

grouped = ttt.groupby(['author_id', 'work_id', 'year', 'language', 'geonames_group'])

df_grouped = pd.DataFrame()
group_number = 1
for name, group in tqdm(grouped, total=len(grouped)):
    if group.shape[0] > 1:
        group['group_number'] = group_number
        group_number += 1
        df_grouped = pd.concat([df_grouped, group])

df_grouped = df_grouped.sort_values(['author_id', 'work_id', 'language', 'year'])

df_grouped.to_excel('possible_duplicates.xlsx', index=False)

















ttt = test.head(100)

translations_df.loc[translations_df['geonames_name'].str.contains('Gossau', na=False)][['geonames_id', 'geonames_name', 'geonames_lat']]
