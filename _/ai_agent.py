# AI Agent
import pandas as pd
from ligonlibrary import authinfo
import openai
import os
from cfe.df_utils import df_to_orgtbl
import json
import re

api_key = "Type your API key here" 
# self.api_key = authinfo.get_password_for_machine('api.openai.com')
class gpt_agent:
    def __init__(self, api_key=api_key):
        self.api_key = api_key
        self.client = openai.OpenAI(api_key=self.api_key)
        self.open_ai_model = 'gpt-4o'
        self.max_tokens = 4096
        self.temperature = 0.3
        self.presence_penalty = 0.1
        self.frequency_penalty = 0.1
        self.agent_role = "You are an expert in text normalization and data cleaning."
    

    def get_payload(self, prompt_text):
        payload={
            'model':"gpt-4",
            'messages': [
                {"role": "system", "content": self.agent_role},
                {"role": "user", "content": prompt_text}
            ],
            "max_tokens": self.max_tokens,  # maximum n umber of tokens to generate
            'temperature': self.temperature,  # Adjust as needed
            'presence_penalty': self.presence_penalty,  # Lower value for focused responses
            'frequency_penalty': self.frequency_penalty  # Lower value to maintain consistency in terms 
               }
        return payload
    
    def get_response(self, payload):
        completion = self.client.chat.completions.create(
            **payload
        )
        # Return text response
        try:
            event = completion.choices[0].message.content
        except Exception as e:
            print(e)
            event = None
        return event
    
    
    def parse_information_with_gpt(self, prompt_text):
        prompt_payload = self.get_payload(prompt_text)
        response = self.get_response(prompt_payload)
        match = re.search(r'json\n({.*?})\n', response, re.DOTALL)
        if match:
            json_str = match.group(1)
            response_converted = json.loads(json_str)
            df = pd.DataFrame(list(response_converted.items()), columns=["Original Label", "Preferred Label"])
            return df

        else:
            print("No JSON response found")

    



class prompt:
    def __init__(self, data):
        self.data = data
    
    def convert_df_to_str(self, data):
        table_str = df_to_orgtbl(data)  # Avoid overriding built-in str
        return table_str
    
    def food_label_prompt(self):
        prompt = f"""
            You are an AI assistant specialized in text data normalization, particularly for food labels in French and English. Your role involves identifying and correcting inconsistencies in food label data to ensure uniformity and accuracy.

            Your task is to:
                1.	Review the provided list of food labels. These labels may contain typos, minor variations in wording, or additional descriptions in parentheses.
                2.	Identify and normalize labels that essentially refer to the same item, despite having:
                    •	Typos (e.g., “Aubergine” vs. “Aubergin”)
                    •	Extra descriptions or details in parentheses that do not change the basic item (e.g., “Pommes (Granny Smith)” should be normalized to “Pommes”).
                    •	Slight wording variations that are commonly known to refer to the same item (e.g., “Citron vert” and “Lime” should both be normalized to “Citron vert”).
                3.	Avoid over-generalization of unique items. Each distinct food item should retain its specific label unless it fits the criteria above. Do not group distinct items under generic labels unless explicitly similar.
                4.	Output the mappings in JSON format, showing the original labels and their normalized forms.

            Here is the list of labels to normalize, please go through one by one and provide the normalized label for each: 
            {self.convert_df_to_str(self.data)}


            Provide the response in JSON format.

            ### **Expected Output Format**
            ```json
            {{
                "Feuilles de Epinar": "Feuilles de Epinar",
                "Feuilles de Fakoye (Feuille de corete)": "Feuilles de Fakoye",
                "Feuilles de baobab": "Feuilles de baobab",
                "Feuilles de patate": "Feuilles de patate",
                "Citron vert": "Citron vert",
                "Lime": "Citron vert"  // Example of handling slight variations
            }}
            """
        return prompt
    
    def unit_prompt(self):
        prompt = f"""
            You are an AI assistant specialized in text data normalization. Your role involves identifying and correcting inconsistencies in unit labels to ensure uniformity and accuracy.
            
            Please process the following list of unit labels and simplify them into more easily understood and standardized unit names.\n
            Convert complex or detailed labels into concise, commonly recognized measurement units. For example:\n
	            •	‘Sack (100 Kg)’ : ‘100 Kg’
	            •	‘Boite de lait concentré’ : ‘Boite'
	            •	‘Carton (Brique)’ : ‘Carton’

            Here is the list of labels to normalize, please go through one by one and provide the normalized label for each: 
            {self.convert_df_to_str(self.data)}


            Provide the response in JSON format.

            ### **Expected Output Format**
            ```json
            {{
               'Sack (100 Kg)': '100 Kg'
                'Sack (25 Kg)':  '25 Kg'  
               'Sack (50 Kg)':  '50 Kg'  
               'Boite de lait concentré'→ 'Boite'   
            }}
            """
        return prompt


def ai_process(data, prompt_method, ai_agent=gpt_agent()):
    df = data
    small_dfs = [df.iloc[i:i + 100] for i in range(0, df.shape[0], 100)]
    normalized_dfs = []
    # Process each small dataframe
    for small_df in small_dfs:
        prompt_instance = prompt(small_df)
        prompt_text = getattr(prompt_instance, prompt_method)()
        normalized_df = ai_agent.parse_information_with_gpt(prompt_text)
        normalized_dfs.append(normalized_df)
        
    result_df = pd.concat(normalized_dfs)
    return result_df