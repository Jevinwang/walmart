
# coding: utf-8

# In[43]:


import numpy as np
import pandas as pd


# In[44]:


file_path = "/home/liurong/Project/Private/hbase_data_processor/files/pro/20190301/log_by_kafka_topic-20190301"


# In[45]:


writer = pd.ExcelWriter('/home/xiaolan/output_0302.xlsx')


# In[46]:


log = pd.read_csv(file_path, sep='|')


# In[41]:


log['log:json'][0]


# In[40]:


log.head()


# In[16]:


import json


# In[23]:


x = '{"ver":1,"mcId":1785,"c":0}'
items = json.loads(x).items()
[v for (k,v) in items]


# In[39]:


def parse_json(x):
    l = list()
    #cols = list()
    for (k,v) in json.loads(x).items():
        l.append(v)
        if k == "eventRes":
            for (ks,vs) in json.loads(v).items():
                l.append(vs)
        if k == "adFrom":
            for (ks,vs) in json.loads(v).items():
                l.append(vs)   
    return l


# In[104]:


t = '{"eventRes-ver":"1","mcId":"1785","unifyId":"","storeId":"","uStoreId":"","appType":"1","appId":"1","prePage":"","curPage":"","pageTime":"","event":"1","eventRes":"{\\"id\\":\\"\\",\\"domPos\\":\\"\\",\\"type\\":\\"\\",\\"typeCN\\":\\"\\",\\"kword\\":\\"\\",\\"cg\\":\\"\\",\\"cgCN\\":\\"\\",\\"sort\\":\\"\\",\\"sortCN\\":\\"\\",\\"state\\":\\"\\",\\"stateCN\\":\\"\\",\\"from\\":\\"\\",\\"fromCN\\":\\"\\",\\"upc\\":\\"\\"}","sessionId":"KA0ivcqXlWvjjc7yGLbgPg\\u003d\\u003d","uid":"","openId":"oY84P5XZ6podFVTAhar1bhTwKbcg","wxGender":"N","phone":"","isNew":"0","screenWidth":"360","screenHeight":"760","screenDesc":"3","screenIsH":"","os":"Android 8.1.0","browser":"Chrome Mobile","network":"","ua":"","devBrand":"HONOR","devMode":"","devLang":"zh_CN","wxVer":"7.0.3","platform":"android","ip":"10.88.96.75","ctime":"2019-02-10 08:37:57","ts":1549759077124,"cts":1549759076011,"env":"1","appFrom":"","adFrom":"{"chan_wxapp_scene":1008,"chan_id":1008}","pageRefer":"","mac":"","prov":"","city":"","addr":"","longitude":"113.30682481553819","latitude":"22.972012803819446","devNo":"","isCrack":"","opt1":"","opt2":"","wxUnionId":"owKUrwznYaK1JahAzcppZbTfhrl4"}'


# In[106]:


c2 = ['id', 'domPos', 'type', 'typeCN', 'kword', 'cg', 'cgCN', 'sort', 'sortCN', 'state', 'stateCN', 'from', 'fromCN', 'upc']
c3 = ['chan_wxapp_scene', 'chan_id']


# In[47]:


def filter_log_df(input, name):
    data_list_value = input[name].apply(lambda x : [ v for (k, v) in json.loads(x).items()])
    #print(data_list_value)
    
    df = pd.DataFrame.from_items(zip(data_list_value.index, data_list_value.values))
    columns = input[name].apply(lambda x : [k for (k, v) in json.loads(x).items()])[0]
    col_dict = {i: columns[i] for i in range(len(columns))}
    dff = df.T.rename(columns=col_dict)
    return dff
    


# In[48]:


dff = filter_log_df(log, 'log:json')


# In[49]:


dff.info()


# In[66]:


eventRes_col = dff['eventRes'].apply(lambda x : ["eventRes-" + k for (k,v) in json.loads(x).items()]).iloc[-1000:,].tolist()[0]


# In[67]:


len(eventRes_col)


# In[68]:


eventRes_col


# In[69]:


eventRes_val = dff['eventRes'].apply(lambda x : [v for (k,v) in json.loads(x).items()])


# In[70]:


eventRes_val[0]


# In[71]:


addfrom_col = dff['adFrom'].apply(lambda x : ["adFrom-" + k for (k,v) in json.loads(x).items()]).iloc[-10:,].tolist()[0]


# In[72]:


for ind, i in enumerate(addfrom_col):
    dff[i] = dff['adFrom'].apply(lambda x : json.loads(x)[i.split('-')[1]] if i.split('-')[1] in json.loads(x).keys() else np.nan)


# In[73]:


for ind, i in enumerate(eventRes_col):
    dff[i] = dff['eventRes'].apply(lambda x : str(json.loads(x)[i.split('-')[1]]) if i.split('-')[1] in json.loads(x).keys() else np.nan)


# In[26]:


len(dff.columns)


# In[33]:


events = dff['event'].unique()


# In[74]:


dff.columns


# In[27]:


dff.head()


# In[75]:


for event in events:
    # queshilv
    total = dff[dff['event'] == event].shape[0]
    isnull_df = (total - dff[dff['event'] == event].isnull().sum()) / total
    nisnull_df = isnull_df.to_frame()
    nisnull_df.rename(columns={0:'Non Null rate'},inplace=True)
    n_unique = dff[dff['event'] == event].nunique()
    nunique_df = n_unique.to_frame()
    nunique_df.rename(columns={0:'Num of Unique'},inplace=True)
    
    res = pd.Series()
    for c in dff.columns:
        common5 = dff[dff['event'] == event][c].value_counts()[:5] / dff[dff['event'] == event][c].count()
        res = res.append(pd.Series(str(common5.to_dict())))
    
    res_df5 = res.to_frame()
    res_df5.rename(columns={0:'Most common 5'},inplace=True)
    res_t = res_df5.T
    res_t.columns = dff.columns
    
    #kongzhilv
    dff_empty = dff.replace(np.nan, "~")
    dff_empty = dff_empty.replace("", np.nan)
    #dff_empty = dff_empty.replace("~", )
    total2 = dff_empty[dff_empty['event'] == event].shape[0]
    isempty_df = (total - dff_empty[dff_empty['event'] == event].isnull().sum()) / total2
    nisempty_df = isempty_df.to_frame()
    nisempty_df.rename(columns={0:'Non Empty rate'},inplace=True)
    
    df_res = nisnull_df.T.append(nunique_df.T).append(res_t).append(nisempty_df.T)
    #print(df_res)
    df_res.T.to_excel(writer,sheet_name='event'+event,header=['Non Null rate','Num of Unique','Most common 5','Non Empty rate'],index=True)


# In[76]:


writer.save()

