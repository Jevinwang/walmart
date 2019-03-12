
# coding: utf-8

# In[ ]:


# Gina's openID:    oY84P5TDrUrUEN9tN02-b2uC_AcM
# Xiaolan's openID:     oY84P5Sgn3H_zpSn8Bn28EVum1_o
# YeYANG's openID:    oY84P5dHvbhUQst-_2rVmiRUnjZ8
# most frequenter's openID:   oY84P5d-SRz4HdnstzVnuLuAXMNI


# In[1]:


import pandas as pd
import numpy as np
from datetime import datetime


# In[2]:


pd.options.display.max_columns=100


# In[3]:


pd.options.display.width = 2000


# In[4]:


pd.options.display.max_colwidth = 1000


# In[5]:


env = 'pro'


# In[6]:


def get_df(date):
    table_log = 'log_by_kafka_topic-' + date
    file_path = '/'.join(['files', env, date, table_log])
    res_log = pd.read_csv(file_path, sep='|')
    return res_log


# In[7]:


def filter_log_df(input_df, date):
    columns = input_df['log:json'].apply(lambda x : [ k for (k, v) in eval(x).items()]).iloc[:1,].tolist()[0]
    data_list = input_df['log:json'].apply(lambda x : [ v for (k, v) in eval(x).items()])
    column_dict = {i: columns[i] for i in range(len(columns))}
    df = pd.DataFrame.from_items(zip(data_list.index, data_list.values))
    df = df.T.rename(columns=column_dict)
#     df['date'] = date
#     df = df.replace('',np.nan).dropna(axis=1, inplace=False)
    return df        


# In[ ]:


for date in range(20190116, 20190125):
    date = str(date)
    df = get_df(date)
    df = filter_log_df(df, date)
    df.to_csv("files/pro/log_by_kafka_topic-" + date + ".csv", index=False)
    print("Finished {}".format(date))


# In[10]:


res = pd.DataFrame()
for date in range(20190117,20190129):
    date = str(date)
    file_path = "/".join(["files", env, "log_by_kafka_topic-" + date + ".csv"])
    df_date = pd.read_csv(file_path)
    res = pd.concat([res, df_date], sort=False)
    print("Finished {}".format(date))


# In[11]:


res.columns


# In[12]:


res.head()


# ### 部分人的session 分析

# In[ ]:


def get_session_info(openID, name):
    session_df = res[res['openId']==openID][['event','ts','date','sessionId']]
    session_df['time'] = session_df['ts'].apply(lambda x: datetime.fromtimestamp(float(x/1000.0)).strftime('%Y-%m-%d %H:%M:%S'))
    session_df['pv'] = session_df['sessionId'].map(session_df.groupby('sessionId')['date'].count().to_dict())
    session_df['time'] = pd.to_datetime(session_df['time'])    
    session_df['valid_period'] = session_df['sessionId'].map(session_df.groupby('sessionId')['time'].agg(np.ptp).to_dict())
    session_df.to_csv('session_out/' + name, index=False)


# In[ ]:


get_session_info("oY84P5TDrUrUEN9tN02-b2uC_AcM", "Gina")


# In[ ]:


get_session_info("oY84P5Sgn3H_zpSn8Bn28EVum1_o", "xiaolan")


# In[ ]:


get_session_info("oY84P5dHvbhUQst-_2rVmiRUnjZ8","ye")


# In[ ]:


get_session_info("oY84P5d-SRz4HdnstzVnuLuAXMNI", "freq")


# In[ ]:


res['openId'].value_counts()


# ### Session Survival Analysis

# In[ ]:


res['time'] = res['ts'].apply(lambda x: datetime.fromtimestamp(float(x/1000.0)).strftime('%Y-%m-%d %H:%M:%S'))


# In[ ]:


res['pv'] = res['sessionId'].map(res.groupby('sessionId')['date'].count().to_dict())


# In[ ]:


res['time'] = pd.to_datetime(res['time']) 


# In[ ]:


res['valid_period'] = res['sessionId'].map(res.groupby('sessionId')['time'].agg(np.ptp).to_dict())


# In[ ]:


out = res[['openId','sessionId', 'pv', 'event','date','time','valid_period']]


# In[ ]:


import matplotlib.pyplot as plt


# In[ ]:


session_df = out[['sessionId','valid_period','pv']].drop_duplicates().dropna()


# In[ ]:


session_df['valid_period'].describe()


# In[ ]:


session_df['pv'].describe()


# In[ ]:


fig, ax = plt.subplots(figsize=(15,7))
session_df.groupby('pv').count()['sessionId'].plot(ax=ax,logx=True)


# In[ ]:


session_df['pv'].plot.hist()


# In[ ]:


session_df.head()


# In[ ]:


session_df['hours'] = session_df['valid_period'].dt.components.hours + session_df['valid_period'].dt.days*24 + session_df['valid_period'].dt.components.minutes/60


# In[ ]:


session_df.head()


# In[ ]:


session_df.shape


# In[ ]:


session_df.hist()


# In[ ]:


session_df.to_csv('session_out/ttl_session.csv', index=False)

