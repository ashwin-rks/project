from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')

engine = create_engine(DATABASE_URL)

query = """
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'public'
"""

table_names = pd.read_sql(query, engine)

current_time = datetime.now()

for table in table_names['table_name']:
    df = pd.read_sql(f'SELECT * FROM public."{table}"', engine)
    
    df['data_loaded_at'] = current_time
    
    file_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data', f'raw_{table}.csv')


    df.to_csv(file_path, index=False)
    print(file_path)
    print(f"Table {table} has been saved with 'data_loaded_at' column as raw_{table}.csv")

engine.dispose()