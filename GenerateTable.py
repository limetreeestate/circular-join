#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import csv
from sys import argv


# In[ ]:


tableSize = argv[2]
saveName = argv[1] + ".csv"


# In[2]:


def Generate(sizeMB: int):
    np.random.seed = 7
    df = pd.DataFrame(np.random.randint(0,100,size=(sizeMB*1024*1024//16, 4)), columns=list('ABCD'))
    return df.astype("int32")


# In[3]:


df = Generate(int(tableSize))


# In[4]:


df.info()


# In[5]:


df.reset_index().to_csv(saveName, index=False)

