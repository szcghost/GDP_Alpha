import sqlite3
import pandas as pd
from datetime import datetime as dt
conn = sqlite3.connect('F:\project_gdp\GD.db')
indexweight = pd.read_sql_query('''select * from indexweight''', conn)
cur = conn.cursor()
cur.execute('''truncate table indexweight''')
conn.commit()
indexweight.index = [dt.strptime(i, "%Y-%m-%d") for i in indexweight.time]
def recreate(df):
    df = df.resample('D').ffill()
    df.loc[:, 'time'] = [dt.strftime(i, "%Y-%m-%d") for i in df.index]
    return df
data = indexweight.groupby(['stkcd', 'indexname']).apply(recreate)
data.index = range(len(data))

import pickle
all_tradedate = pickle.load(open('F:\百度云同步盘\旦复旦的学习\Quant\GDP\PJ\pickle\\all_tradedate', 'rb'))
data = data[data.time.isin(all_tradedate.time)]
data.to_sql('indexweight', conn, if_exists='append', index=False, index_label=None, chunksize=1000)