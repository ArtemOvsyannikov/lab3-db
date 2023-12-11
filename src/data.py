import os
import pandas as pd
import gdown

def download_dataset():
    if not os.path.exists('data/tiny.csv'):
        if not os.path.exists('data'):
            os.makedirs('data')

        url = 'https://drive.google.com/uc?export=download&id=1XWCk4XmgdNUZ8E42ktjGpeeKZeTO9YnJ'
        output = 'data/tiny.csv'
        gdown.download(url, output, quiet=False)

def load_data():
    df = pd.read_csv('data/tiny.csv')
    if 'Airport_fee' in df.columns:
        df = df.drop(columns=['Airport_fee'])
    return df 