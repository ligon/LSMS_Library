# AI Agent
import pandas as pd
from ligonlibrary import authinfo
import openai
import os
import json
import re
from .categorical_prompt import prompt



# api_key = "Type your API key here" 
api_key = authinfo.get_password_for_machine('api.openai.com')
class gpt_agent:
    def __init__(self, api_key=api_key):
        self.api_key = api_key
        self.client = openai.OpenAI(api_key=self.api_key)
        self.open_ai_model = 'gpt-4o'
        self.max_tokens = 4096
        self.temperature = 0.3
        self.presence_penalty = 0.1
        self.frequency_penalty = 0.1
        self.prompt = prompt()
    

    def get_payload(self, data, base_prompt):
        if base_prompt == 'harmonize_food_label':
            prompt = self.prompt.harmonize_food_label_prompt()
            data = self.food_label_dict(data)
        elif base_prompt == 'aggregate_food_label':
            prompt = self.prompt.aggregate_food_label_prompt()
            data = self.food_label_list(data)
        elif base_prompt == 'unit_prompt':
            prompt = self.prompt.unit_prompt()
            data = self.unit_labels_list(data)
        else:
            raise ValueError("No prompt provided")

        payload = {
            'model': self.open_ai_model,
            "messages":[
                    {"role": "system", "content": "You are an expert in food label harmonization."},
                    {"role": "user", "content": [{"type": "text", "text": prompt}]}
                ],
            "max_tokens": self.max_tokens,  
            'temperature': self.temperature,  
            'presence_penalty': self.presence_penalty, 
            'frequency_penalty': self.frequency_penalty,  
        }
        if base_prompt == 'harmonize_food_label':
            for num, val in enumerate(data):
                payload['messages'].append({"role": "user", "content": [{"type": "text", "text": f"The {num+1} input is {val} wave and food label is : {data[val]}"}]})
        return payload

    def parse_information_with_gpt(self, data, base_prompt):
        payload = self.get_payload(data, base_prompt)
        response = self.make_api_call(payload)
        #safe load:
        try:
            final_response = json.loads(response.content)
        except json.JSONDecodeError as e:
            final_response = str(response.content)
        return final_response
    
    def make_api_call(self, payload):
        completion =self.client.chat.completions.create(**payload)
        try:
            event = completion.choices[0].message
        except Exception as e:
            print(e)
            event = None
        return event
    
    def food_label_list(self, data):
        if isinstance(data, dict):
            return data
        elif isinstance(data, pd.DataFrame):
            return list(data.index.get_level_values('j').unique())
        else:
            raise ValueError("No data provided")
    
    def food_label_dict(self, data):
        if isinstance(data, dict):
            return data
        elif isinstance(data, pd.DataFrame):
            food_labels_dict = {}
            years = list(data.index.get_level_values('t').unique())
            for year in years:
                t_label = list(data.xs(year, level='t').index.get_level_values('j').unique())
                food_labels_dict[year]  =t_label
    
            return food_labels_dict
        else:
            raise ValueError("No data provided")
    
    def unit_labels_list(self, data):
        if isinstance(data, dict):
            return data
        elif isinstance(data, pd.DataFrame):
            return list(data.index.get_level_values('u').unique())
        else:
            raise ValueError("No data provided")
        
        



# def ai_process(data, prompt_method, ai_agent=gpt_agent()):
#     if isinstance(data, pd.DataFrame):
#         prompt_instance = prompt(food_acquired_df=data)
#     elif isinstance(data, dict) or isinstance(data, list):
#         prompt_instance = prompt(food_acquired_df=None, direct_data=data)
#     prompt_text = getattr(prompt_instance, prompt_method)()
#     normalized_df = ai_agent.get_response(prompt_text)
#     return normalized_df


# def aggregate_labels(df):
#     result = ai_process(df, 'aggregate_food_label_prompt')
#     return result

# def aggregate_units(df):
#     result = ai_process(df, 'unit_prompt')
#     return result

# def harmonize_food_label(df):
#     '''
#     preferred labels
#     '''
#     result = ai_process(df, 'harmonize_food_label_prompt')
#     return result


# fdc_apikey = "RlA69gtW6YXWZIc4BzEOPGdpBrV877Lp8wOeU1Os"  # Replace with a real key!  "DEMO_KEY" will be slow...
# import fooddatacentral as fdc

# def fct_nutrients(preferred_food_label_list, fdc_apikey=fdc_apikey):
#     useful_information = ['fdcId', 'description', 'additionalDescriptions', 'ingredients', 'packageWeight', 'servingSizeUnit', 'servingSize']
#     fdc_mapping = {}
#     for food in preferred_food_label_list:
#         fdc_options = fdc.search(food) 
#         fdc_information = fdc_options[useful_information].set_index('fdcId').to_dict(orient='records')
#         fdc_mapping[food]['fdcIds'] = list(fdc_options['fdcId'])
#         best_id = ai_process({food:fdc_information}, 'fct_nutrients_mapping_prompt')
#         fdc_mapping[food]['preferred_fdcid'] = best_id
#         fdc_mapping[food]['fdc_id_info'] = fdc_information[best_id]

#     return fdc_mapping