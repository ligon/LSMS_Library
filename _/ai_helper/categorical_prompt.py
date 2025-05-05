# AI Agent
import pandas as pd
# from cfe.df_utils import df_to_orgtbl

class prompt:
    
    def aggregate_food_label_prompt(self):
        '''
        Working with food_labels_list to normalize food labels to aggregate food labels
        food_labels_list = [
            "Feuilles de Epinar",
            "Feuilles de Fakoye (Feuille de corete)",
            ...]
        The food_labels_list can be a list of preferred labels or all food labels across all waves
        '''
        prompt = f"""
            You are an AI assistant specialized in text data normalization, particularly for food labels in any languages (English/French). Your role involves identifying and correcting inconsistencies in food label data to ensure uniformity and accuracy.

            Your task is to:
                1.	Review the provided list of food labels. These labels may contain typos, minor variations in wording, or additional descriptions in parentheses.
                2.	Identify and normalize labels that essentially refer to the same item, despite having:
                    •	Typos (e.g., “Aubergine” vs. “Aubergin”)
                    •	Extra descriptions or details in parentheses that do not change the basic item (e.g., “Pommes (Granny Smith)” should be normalized to “Pommes”).
                    •	Slight wording variations that are commonly known to refer to the same item (e.g., “Citron vert” and “Lime” should both be normalized to “Citron vert”).
                3.	Avoid over-generalization of unique items. Each distinct food item should retain its specific label unless it fits the criteria above. Do not group distinct items under generic labels unless explicitly similar.
                4.	Output the mappings in JSON format, showing the original labels and their normalized forms.

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
        '''
        Woking on unit_labels_list which aggregates all years food labels into a single list
        
        unit_labels_list = [
            "Sack (100 Kg)",
            "Sack (25 Kg)",
            ...]
        '''

        prompt = f"""
            You are an AI assistant specialized in text data normalization. Your role involves identifying and correcting inconsistencies in unit labels to ensure uniformity and accuracy.
            
            Please process the following list of unit labels and simplify them into more easily understood and standardized unit names.\n
            Convert complex or detailed labels into concise, commonly recognized measurement units. For example:\n
	            •	‘Sack (100 Kg)’ : ‘100 Kg’
	            •	‘Boite de lait concentré’ : ‘Boite'
	            •	‘Carton (Brique)’ : ‘Carton’

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
    

    def harmonize_food_label_prompt(self):
        prompt = (
            "You are a specialized food data harmonization assistant tasked with creating a comprehensive mapping of food labels across multiple survey years.\n\n"
            "## INSTRUCTION\n\n"
            "Process ALL food labels from every year and create a complete normalized mapping that identifies identical food items despite variations in:\n"
            "- Spelling or typos\n"
            "- Capitalization differences\n"
            "- Word order changes\n"
            "- Descriptive text variations\n"
            "- Formatting differences (parentheses, commas, hyphens)\n"
            "- Singular/plural forms\n\n"
            "## CRITICAL REQUIREMENTS\n\n"
            "1. You MUST examine EVERY SINGLE food label in EVERY year\n"
            "2. You MUST include ALL food items in your output (no omissions)\n"
            "3. You MUST NOT truncate the results - return the FULL harmonized list\n"
            "4. Handle large datasets completely without cutting off any entries\n\n"
            "## OUTPUT FORMAT\n\n"
            "Return a JSON array of dictionaries with:\n"
            "- \"preferred_label\": Choose a clear, representative name for the food item\n"
            "- One key for each survey year (e.g., \"2004-05\", \"2010-11\", etc.)\n"
            "- The exact original label text as value for each year, or \"---\" if missing\n\n"
            "Example output:\n"
            "[\n"
            "  {\n"
            "    \"preferred_label\": \"Maize flour\",\n"
            "    \"2004-05\": \"Maize ufa mgaiwa\",\n"
            "    \"2010-11\": \"Maize ufa mgaiwa (normal flour)\",\n"
            "    \"2013-14\": \"Maize ufa refined (fine flour)\"\n"
            "  },\n"
            "  {\n"
            "    \"preferred_label\": \"Avocado\",\n"
            "    \"2004-05\": \"Avocado\",\n"
            "    \"2010-11\": \"---\",\n"
            "    \"2013-14\": \"avocado\"\n"
            "  }\n"
            "]\n\n"
            "## STRICT OUTPUT RULES\n\n"
            "- Return ONLY the raw JSON array with no markdown, code fences, explanations, or other text\n"
            "- Every dictionary must include \"preferred_label\" and ALL year keys\n"
            "- For years where an item doesn't appear, use \"---\" as the value\n"
            "- Do not include entries where ALL year values are \"---\"\n"
            "- Ensure the output is valid JSON format (proper quotes, commas, brackets)\n"
            "- If the dataset is large, process the ENTIRE dataset (do not truncate)\n"
        )
        return prompt
    
    def fct_nutrients_mapping_prompt(self):
        prompt = f"""
                You are an expert in food taxonomy and database linking. Your task is to select the most appropriate FDC food entry for a given preferred food label, based on a search result from USDA's FoodData Central.

                Each result contains:
                - `description`: the main food name.
                - `additionalDescriptions`: optional synonyms or additional information.

                Choose the **single most relevant FDC entry** (returning its `fdcId`) that best matches the preferred food label semantically, based on description clarity, specificity, and synonym match. If no suitable match is found, return "None".
                Return only the best matched fdcId (e.g., 2710006) or "None".
                """        
        return prompt
