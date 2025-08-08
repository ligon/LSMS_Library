import pandas as pd
import numpy as np
import re
import re
import unicodedata
from textblob import TextBlob

def food_label_dict(data):
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

def regularize_string(s):
    # Normalize and remove non-printable/non-UTF8-like characters
    s = ''.join(c for c in s if ord(c) < 128)

    # Trim whitespace
    s = s.strip()

    # Reduce multiple spaces to single spaces
    s = re.sub(r'\s+', ' ', s)

    # Handle "x, y" -> "x (y)" and "x - y" -> "x (y)" transformations
    s = re.sub(r'(\w+),\s*(\w+)', r'\1 (\2)', s)
    s = re.sub(r'(\w+)\s*-\s*(\w+)', r'\1 (\2)', s)

    # Convert "and" to "&"
    s = re.sub(r'\band\b', '&', s, flags=re.IGNORECASE)

    # List of minor words to keep lowercase (unless they're the first word)
    minor_words = {'a', 'an', 'the', 'and', 'but', 'or', 'for', 'nor',
                   'on', 'in', 'at', 'to', 'by', 'of'}

    words = s.split(' ')
    for i, word in enumerate(words):
        if word.startswith('(') and word.endswith(')'):
            words[i] = word.lower()
        elif '-' in word:
            parts = word.split('-')
            words[i] = '-'.join(p.capitalize() for p in parts)
        elif i == 0 or word.lower() not in minor_words:
            words[i] = word.capitalize()
        else:
            words[i] = word.lower()

    return ' '.join(words)

def preprocess(label):
    useless_term = ['etc.']
    # Remove leading and trailing whitespace
    label = regularize_string(label)
    # label = str(TextBlob(label).correct())
    label = label.strip()
    label = label.lower()
    # Remove useless terms
    for term in useless_term:
        label = label.replace(term, '')
    label = re.sub(r'[^\w\s]', ' ', label)  # remove punctuation
    #remove more than one space
    label = re.sub(r'\s+', ' ', label)
    return label.split()



from sklearn.metrics.pairwise import cosine_similarity
# Convert label to vector using Word2Vec model
def get_label_vector(label, model):
    words = preprocess(label)
    word_vectors = [model.wv[word] for word in words if word in model.wv]
    if word_vectors:
        return np.mean(word_vectors, axis=0)
    else:
        return np.zeros(model.vector_size)
    
# Calculate cosine similarity between two labels based on a model
def get_cosine_similarity(label1, label2, model):
    vector1 = get_label_vector(label1, model)
    vector2 = get_label_vector(label2, model)
    similarity = cosine_similarity([vector1], [vector2])[0][0]
    return similarity

def group_labels(labels, threshold=0.8):
    """
    Groups labels based on cosine similarity.
    Args:
        labels (list): List of labels to group.
        threshold (float): Cosine similarity threshold for grouping.
    Returns:
        list: List of grouped labels and their corresponding wave.
    """
    grouped = []
    used = set()

    for i, item in enumerate(labels):
        if i in used:
            continue
        group = [item]
        used.add(i)

        for j, other in enumerate(labels):
            if j in used or i == j:
                continue
            sim = cosine_similarity([item['vector']], [other['vector']])[0][0]
            if sim > threshold:
                group.append(other)
                used.add(j)
        grouped.append(group)
    return grouped

def parse_output(grouped):
    """
    Parses the grouped labels into a readable format.
    Args:
        grouped (list): List of grouped labels.
    Returns:
        list: List of dictionaries with wave and label information.
    """
    output = []
    for group in grouped:
        group_dict = {}
        for item in group:
            group_dict[item['wave']] = item['label']
        output.append(group_dict)
    return output


########################################################################################
########################################################################################
########################################################################################
# Example usage
# Using Malawi Data as the word corpus
import lsms_library as ll
malawi = ll.Country('Malawi')
food_data = malawi.food_acquired()
food_dict = food_label_dict(food_data)
word_corpus = [preprocess(label) for labels in food_dict.values() for label in labels]


from gensim.models import Word2Vec
# Train Word2Vec model
model = Word2Vec(word_corpus, vector_size=100, window=3, min_count=1, workers=4)

# Save the model
model.save("malawi_food_labels.model")
# Load the model
model = Word2Vec.load("malawi_food_labels.model")

all_labels = []
for wave, labels in food_dict.items():
    for label in labels:
        all_labels.append({'wave': wave, 'label': label, 'vector': get_label_vector(label, model)})

      
grouped = group_labels(all_labels, threshold=0.8)
output = parse_output(grouped)
df = pd.DataFrame(output)
# df = ll.local_tools.write_df_to_org(df, 'grouped_labels', 'grouped_labels')

