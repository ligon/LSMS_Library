
import pandas as pd
from ligonlibrary import authinfo
import openai
import os
import cfe.df_utils

openai_key= authinfo.get_password_for_machine('api.openai.com')

def get_openai_response(prompt_text, openai_key):
    # Initialize OpenAI client
    client = openai.OpenAI(api_key=openai_key)

    # Call OpenAI GPT-4
    response = client.chat.completions.create(
        model="gpt-4",
        messages=[
            {"role": "system", "content": "You are an expert in text normalization and data cleaning."},
            {"role": "user", "content": prompt_text}
        ],
        temperature=0.3,  # Lower temperature ensures consistency
    )

    # Return text response
    return response.choices[0].message.content


# Define the prompt clearly
def prompt(str):
    return f"""
            You are an AI assistant specialized in data normalization, particularly for food labels in French. Your role involves identifying and correcting inconsistencies in food label data to ensure uniformity and accuracy.

            Your task is to:
                1.	Review the provided list of food labels in French. These labels may contain typos, minor variations in wording, or additional descriptions in parentheses.
                2.	Identify and normalize labels that essentially refer to the same item, despite having:
                    •	Typos (e.g., “Aubergine” vs. “Aubergin”)
                    •	Extra descriptions or details in parentheses that do not change the basic item (e.g., “Pommes (Granny Smith)” should be normalized to “Pommes”).
                    •	Slight wording variations that are commonly known to refer to the same item (e.g., “Citron vert” and “Lime” should both be normalized to “Citron vert”).
                3.	Avoid over-generalization of unique items. Each distinct food item should retain its specific label unless it fits the criteria above. Do not group distinct items under generic labels unless explicitly similar.
                4.	Output the mappings in JSON format, showing the original labels and their normalized forms.

            Here is the list of labels to normalize, please go through one by one and provide the normalized label for each: 
            {str}


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

import re
import json

def convert_response(response):
    match = re.search(r'json\n({.*?})\n', response, re.DOTALL)
    if match:
        json_str = match.group(1)
        response_converted = json.loads(json_str)
        return pd.DataFrame(list(response_converted.items()), columns=["Original Label", "Normalized Label"])

    else:
        return "No JSON response found"


def write_two_org_tables(label_df, unit_df, filename):
    with open(filename, "w", encoding="utf-8") as file:
        file.write("#+TITLE: Categorical Mapping\n\n")

        # Write the first table (label)
        file.write("* Food Labels\n\n")
        file.write("#+NAME: harmonize_food\n")
        file.write(cfe.df_utils.df_to_orgtbl(label_df))  # Convert label DataFrame to Org table
        file.write("\n\n")  # Add spacing

        # Write the second table (unit)
        file.write("* Unit Information\n\n")
        file.write("#+NAME: unit\n")
        file.write(cfe.df_utils.df_to_orgtbl(unit_df)) 
