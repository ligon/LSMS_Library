
import dvc.api
from lsms import from_dta
from lsms_library.local_tools import category_union, category_remap
import pandas as pd
from lsms_library.label_ai import get_openai_response, prompt, convert_response, write_two_org_tables
import cfe.df_utils
from ligonlibrary import authinfo
import openai

waves = ['2014-15', '2018-19', '2021-22']
files = ['EACIALI_p1.dta','s07b_me_mli2018.dta','s07b_me_mli2021.dta']


def read_data(file):
    with dvc.api.open(file,mode='rb') as dta:
        d = from_dta(dta,categories_only=True)
    return d

wave_1 = read_data(f"{waves[0]}/Data/{files[0]}")
wave_3 = read_data(f"{waves[1]}/Data/{files[1]}")
wave_4 = read_data(f"{waves[2]}/Data/{files[2]}")
harmonize_food = [wave_1['s13q01'], wave_3['s07bq01'], wave_4['s07bq01']]
units = [wave_1['s13q03b'], wave_3['s07bq07b'], wave_4['s07bq07b']]


# delete all spaces before and after the string in dictionary values in the list
harmonize_food = [{k: v.strip() for k, v in d.items()} for d in harmonize_food]
units = [{k: v.strip() for k, v in d.items()} for d in units]

union, t_wave_1, t_wave_3, t_wave_4 = category_union(harmonize_food)
df = pd.DataFrame(list(union.items()), columns = ['Code','Union Label'])
label = df[['Union Label']].sort_values(by='Union Label').reset_index(drop=True)

union, t_wave_1, t_wave_3, t_wave_4 = category_union(units)
df = pd.DataFrame(list(union.items()), columns = ['Code','Union Label'])
units = df[['Union Label']].sort_values(by='Union Label').reset_index(drop=True)


units = units.drop_duplicates()
label = label.drop_duplicates()
label_1 = label[:100]
label_2 = label[100:]
org_units_str = cfe.df_utils.df_to_orgtbl(units)
org_label_str_1 = cfe.df_utils.df_to_orgtbl(label_1)
org_label_str_2 = cfe.df_utils.df_to_orgtbl(label_2)


openai_key= authinfo.get_password_for_machine('api.openai.com')

prompt_text_unit = prompt(org_units_str)
prompt_text_label_1= prompt(org_label_str_1)
prompt_text_label_2= prompt(org_label_str_2)

result_unit = get_openai_response(prompt_text_unit, openai_key)
result_label_1 = get_openai_response(prompt_text_label_1, openai_key)
result_label_2 = get_openai_response(prompt_text_label_2, openai_key)


converted_unit = convert_response(result_unit)
converted_label_1 = convert_response(result_label_1)
converted_label_2 = convert_response(result_label_2)
converted_label = pd.concat([converted_label_1, converted_label_2]).reset_index(drop=True)

write_two_org_tables(converted_label, converted_unit, "categorical_mapping.org")