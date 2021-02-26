#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import csv
from threading import Thread
import time
from sys import argv
import io


# In[2]:


logStr = ""

def log(*param, logMode=True):
    global logStr
    s = ""
    for p in param:
        s += str(p) + " "
    logStr += s + "\n"
    if logMode:
        print(s)


# In[ ]:


tableCount = int(argv[2])
tablePath = argv[1]


# In[11]:


df = pd.read_csv(tablePath).astype("int32")


buffer = io.StringIO()
df.info(buf=buffer)
s = buffer.getvalue()
log(s)


# In[15]:


def share(t: float):
    time.sleep(t)


# In[26]:


def join(A: pd.DataFrame, B: pd.DataFrame, col: str, ret: list):
    share(A.memory_usage().sum()/1024**2/25)
    result = pd.merge(A, B, on=col)
    ret += [[set(result["index"])]] * tableCount


# In[3]:


joins = []
log("Start")
join(df, df, "index", joins)

joinTime = time.time() - start
log("Join time:", joinTime, "seconds")


# In[ ]:


current = joins[0][0]
common = []
for i in range(len(joins)):
    nextDF = joins[0][0] if i+1==len(joins) else joins[i+1][0]
    current = current.intersection(nextDF)

for i in range(len(joins)):
    nextDF = joins[0][0] if i+1==len(joins) else joins[i+1][0]
    current = current.intersection(nextDF)
    common.append(current)
    
totalTime = time.time() - start
log("Total time:", totalTime, "seconds")


# In[ ]:


with open(f"log_P{tablePath.strip('data').strip('.csv')}_N{tableCount}.txt", 'w') as logfile:
    logfile.write(logStr)


# In[ ]:




