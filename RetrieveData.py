import pandas as pd
from sqlalchemy import create_engine
import psycopg2 as pg2

def extract():
    return pd.read_csv('work.csv')

def transform(df):
    df['style'].fillna('not designed',inplace=True)
    df['museum_id'].fillna(0,inplace=True)
    df['museum_id'].astype(int)
    return df

def load(df):
    alchemyEngine = create_engine('postgresql+psycopg2://postgres:nik123@127.0.0.1:5434/painting',pool_recycle=3600)
    conn = alchemyEngine.connect()
    data = df.to_sql('work', con=conn, if_exists = 'replace', index = False)
    dis = pd.read_sql('work',conn)
    print(dis)
    ret = df.iloc[10:20]
    print(ret)
   # aldis = df['work_id'] = 24532
   # print(aldis)
   # print(df.head(10))
   # print(pd.isnull(df['name']))

if __name__ == "__main__":
    data = extract()
    transformed_data = transform(data)
    load(transformed_data)



