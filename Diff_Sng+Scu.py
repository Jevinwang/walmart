
# coding: utf-8

# In[1]:


import pandas as pd
import matplotlib.pyplot as plt
import numpy as np


# In[13]:


sample = pd.read_csv('/u1/liurong/Projects/Private/sng_order_analysis_since_last_SEP.csv', compression='gzip')


# In[10]:


from sklearn.preprocessing import LabelEncoder


# In[12]:


user_id_encoder = LabelEncoder()


# In[14]:


user_id_encoder = LabelEncoder()
sample['user_id'] = user_id_encoder.fit_transform(sample['user_id'])

order_id_encoder = LabelEncoder()
sample['order_id'] = user_id_encoder.fit_transform(sample['order_id'])


# In[17]:


sample.to_csv("/home/xiaolan/wj/wj_xiaolan.csv")


# In[4]:





# In[276]:


sample['time_slot'] = sample['created_time'].apply(lambda x : x.split(" ")[1].split(":")[0])


# In[79]:


sample.drop('time_slot', axis=1, inplace=True)


# In[82]:


sample.head()


# In[ ]:


#1 求total users


# In[3]:


sample[sample['order_type']==4]['user_id'].count()
set1 = set(sample[sample['order_type']==1]['user_id'])
set4 = set(sample[sample['order_type']==4]['user_id'])


# In[4]:


set1_4 = set1.intersection(set4)


# In[6]:



len(set1_4)


# In[7]:


len(set1)


# In[253]:


plt.figure(figsize=(9,6))
x=[0,0.3]
y=np.array([len(set1), len(set4)])
xtricks = ['sng user', 'self-checkout user']
plt.bar(0, len(set1),width=0.2,align='center',alpha=0.8,color='r')
plt.bar(0.3, len(set4),width=0.2,align='center',alpha=0.8,color='c')
#plt.bar(0.6, len(set1_4),width=0.2,align='center',alpha=0.8,color='blueviolet')
plt.title('total users from channels')
plt.xticks(x,xtricks,rotation=20,fontsize=12)
for a,b in zip(x,y):
    plt.text(a,b+0.03,'%.0f' % b,ha='center',va='bottom',fontsize=12)
plt.show()


# In[ ]:


#2 order distribute


# In[254]:


# 订单分布
distribute_order = sample.loc[:,['user_id','order_id']].drop_duplicates()['user_id'].value_counts()
distribute_order.to_frame()['user_id'].value_counts().plot(kind='line',title='order distribute')


# In[255]:


distribute_order_self = sample[sample['order_type'] == 4].loc[:,['user_id','order_id']].drop_duplicates()['user_id'].value_counts()

dis_self = pd.DataFrame({'num':distribute_order_self.to_frame()['user_id'].value_counts().index, 'total':distribute_order_self.to_frame()['user_id'].value_counts().values})


# In[286]:


sample[sample['order_type'] == 4].loc[:,['user_id','order_id']].drop_duplicates()['order_id'].count()


# In[287]:


357337/169974


# In[ ]:


#求1单/2单/大于等于3单的分布


# In[ ]:


#3 repeated pattern
#3.1 ratio of repeated purchase


# In[258]:


sample_rp_all = sample.loc[:,['user_id','order_id','created_time','order_type']].drop_duplicates(subset=['order_id','user_id','order_type'])
sample_rp_all['date'] = sample_rp_all['created_time'].apply(lambda x: datetime.datetime.strptime(x.split(" ")[0],"%Y-%m-%d"))
sng_rp = sample_rp_all[sample_rp_all['order_type']==1].groupby('user_id')['date'].agg(list)
self_rp = sample_rp_all[sample_rp_all['order_type']==4].groupby('user_id')['date'].agg(list)


# In[259]:


user_list_sng = []
for i in range(sng_rp.count()):
    if len(sng_rp[i]) > 1:
        sng_rp[i].sort()
        if (sng_rp[i][1]-sng_rp[i][0]).days < 31:
            user_list_sng.append(sng_rp.index[i])


# In[260]:


user_list_self = []
for i in range(self_rp.count()):
    if len(self_rp[i]) > 1:
        self_rp[i].sort()
        if (self_rp[i][1]-self_rp[i][0]).days < 31:
            user_list_self.append(self_rp.index[i])


# In[261]:


selfr = len(user_list_self) / self_rp.count()
sngr = len(user_list_sng) / sng_rp.count()


# In[262]:


plt.figure(figsize=(10,8))
x=[0,0.3]
y=np.array([sngr, selfr])
print(y)
xtricks = ['sng users', 'self-checkout users']
plt.bar(0, sngr,width=0.2,align='center',alpha=0.8,color='r')
plt.bar(0.3, selfr,width=0.2,align='center',alpha=0.8,color='c')
#plt.bar(0.6, len(set1_4),width=0.2,align='center',alpha=0.8,color='blueviolet')
plt.title('ratio of repeated purchase after Sep 2018')
plt.xticks(x,xtricks,rotation=20,fontsize=12)
for a,b in zip(x,y):
    plt.text(a,b+0.005,'%.3f' % b,ha='center',va='bottom',fontsize=12)
plt.show()


# In[ ]:


#3.2 average repeated cycle


# In[263]:


new_sample = sample_rp_all[sample_rp_all['user_id'].isin(user_list)]


# In[292]:


sng_new = new_sample[new_sample['order_type']==1][new_sample['user_id'].isin(user_list_sng)].groupby('user_id')['date'].agg(list)
days = []
test = []
for i in range(sng_new.count()):
    sng_new[i].sort()
    tmp = 0
    #test.append(sng_new[i])
    for j in range(len(sng_new[i]) - 1):
        #t = sng_new[i][j]
        #test.append(t)
        tmp += (sng_new[i][j+1]-sng_new[i][j]).days
    days.append(tmp/(len(sng_new[i]) - 1))


# In[293]:


self_new = new_sample[new_sample['order_type']==4][new_sample['user_id'].isin(user_list_self)].groupby('user_id')['date'].agg(list)
days2 = []
for i in range(self_new.count()):
    self_new[i].sort()
    tmp = 0
    #test.append(sng_new[i])
    for j in range(len(self_new[i]) - 1):
        #t = sng_new[i][j]
        #test.append(t)
        tmp += (self_new[i][j+1]-self_new[i][j]).days
    days2.append(tmp/(len(self_new[i]) - 1))


# In[291]:


days.clear()
days2.clear()


# In[294]:


d_sng = sum(days) / len(days)
d_self = sum(days2) / len(days2)


# In[295]:


plt.figure(figsize=(10,8))
x=[0,0.3]
y=np.array([d_sng, d_self])
xtricks = ['sng users', 'self-checkout users']
plt.bar(0, d_sng,width=0.2,align='center',alpha=0.8,color='r')
plt.bar(0.3, d_self,width=0.2,align='center',alpha=0.8,color='c')
#plt.bar(0.6, len(set1_4),width=0.2,align='center',alpha=0.8,color='blueviolet')
plt.title('average repeated cycle (days)')
plt.xticks(x,xtricks,rotation=20,fontsize=12)
for a,b in zip(x,y):
    plt.text(a,b+0.005,'%.3f' % b,ha='center',va='bottom',fontsize=12)
plt.show()


# In[ ]:


# 3.3 frequency per month


# In[268]:


new_sample2_sng = new_sample[new_sample['order_type']==1][new_sample['user_id'].isin(user_list_sng)]
new_sample2_sng['month'] = new_sample2_sng.date.apply(lambda x: str(x).split("-")[1])
new_sample2_sng_u = new_sample2_sng.drop_duplicates(subset=['user_id','month'])['month'].value_counts()
fpm = new_sample2_sng['month'].value_counts() / new_sample2_sng_u


# In[269]:


new_sample2_self = new_sample[new_sample['order_type']==4][new_sample['user_id'].isin(user_list_sng)]
new_sample2_self['month'] = new_sample2_self.date.apply(lambda x: str(x).split("-")[1])
new_sample2_self_u = new_sample2_self.drop_duplicates(subset=['user_id','month'])['month'].value_counts()
fpm_self = new_sample2_self['month'].value_counts() / new_sample2_self_u
fpm_self.sort_index()


# In[270]:


plt.figure(figsize=(14,6),dpi=80)
x=range(0,5)
y_1=[3.00,2.79,2.91,2.90,2.12]
y_2=[1.91,2.14,2.35,2.10,1.75]
xtricks = ["10","11","12","01","02"]
plt.plot(x,y_1,label='sng',color='red')
plt.plot(x,y_2,label='self_checkout',color='c')
plt.legend(loc='upper left')
#plt.bar(0.6, len(set1_4),width=0.2,align='center',alpha=0.8,color='blueviolet')
plt.title('Frequency per month')
plt.xticks(x,xtricks,fontsize=10,rotation=30)
plt.grid(alpha=0.4,linestyle=':')
for a,b,c in zip(x,y_1,y_2):
    plt.text(a,b+0.03,'%.0f' % b,ha='right',va='top',fontsize=8)
    plt.text(a,c+0.03,'%.0f' % c,ha='left',va='top',fontsize=8)
plt.show()


# In[ ]:


#4 average basket size


# In[271]:


self_all = sample[sample['order_type']==4]['order_id'].count()
sng_all = sample[sample['order_type']==1]['order_id'].count()
self = sample[sample['order_type']==4]['order_id'].drop_duplicates().count()
sng = sample[sample['order_type']==1]['order_id'].drop_duplicates().count()


# In[282]:


self


# In[272]:


sng_avg_basket_size = sng_all/sng
self_avg_basket_size = self_all / self


# In[273]:


plt.figure(figsize=(8,6.5))
x=[0,0.3]
y=np.array([sng_avg_basket_size, self_avg_basket_size])
xtricks = ['sng_avg_basket_size', 'self-checkout_avg_basket_size']
plt.bar(0, sng_avg_basket_size,width=0.2,align='center',alpha=0.8,color='r')
plt.bar(0.3, self_avg_basket_size,width=0.2,align='center',alpha=0.8,color='c')
plt.title('average basket size from sng & self-checkout')
plt.xticks(x,xtricks,rotation=20)
for a,b in zip(x,y):
    plt.text(a,b+0.03,'%.2f' % b,ha='center',va='bottom',fontsize=12)
plt.show()


# In[ ]:


#5 peak order time


# In[277]:


sng_timeslot = sample[sample['order_type']==1].drop_duplicates('order_id')['time_slot'].value_counts().sort_index()


# In[278]:


self_timeslot = sample[sample['order_type']==4].drop_duplicates('order_id')['time_slot'].value_counts().sort_index()


# In[279]:


time_slot = [ i+"-" for i in self_timeslot.index]
time_slot = [ i + "-" + str(int(i)+1) for i in self_timeslot.index]
time_slot = [i.split('-')[0] + '-0' + i.split('-')[1] if len(i.split('-')[1]) == 1 else i for i in time_slot]


# In[280]:


self_timeslot.values


# In[281]:


plt.figure(figsize=(14,6),dpi=80)
x=range(0,24)
y_1=self_timeslot.values
y_2=sng_timeslot.values
xtricks = time_slot
plt.plot(x,y_1,label='self_checkout',color='red')
plt.plot(x,y_2,label='sng',color='c')
plt.legend(loc='upper left')
#plt.bar(0.6, len(set1_4),width=0.2,align='center',alpha=0.8,color='blueviolet')
plt.title('Peak Order Time')
plt.xticks(x,xtricks,fontsize=10,rotation=30)
plt.grid(alpha=0.4,linestyle=':')
for a,b,c in zip(x,y_1,y_2):
    plt.text(a,b+0.03,'%.0f' % b,ha='right',va='top',fontsize=8)
    plt.text(a,c+0.03,'%.0f' % c,ha='left',va='top',fontsize=8)
plt.show()

