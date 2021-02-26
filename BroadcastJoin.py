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


# In[3]:


tableCount = int(argv[2])
tablePath = argv[1]


# In[4]:


df = pd.read_csv(tablePath).astype("int32")


buffer = io.StringIO()
df.info(buf=buffer)
s = buffer.getvalue()
log(s)


# In[5]:


def share(t: float):
    time.sleep(t)


# In[7]:


def join(A: pd.DataFrame, B: pd.DataFrame, col: str, ret: list):
    result = pd.merge(A, B, on=col)
    ret += [result]


# In[ ]:


val = [df]
log("Start")
start = time.time()
for i in range(tableCount):
    share(df.memory_usage().sum()/1024**2/25*(tableCount-1))
    join(val[-1], df, "index", val)
totalTime = time.time() - start
log("Total time:", totalTime, "seconds")


# In[ ]:


with open(f"Broadcast_log_P{tablePath.strip('data').strip('.csv')}_N{tableCount}.txt", 'w') as logfile:
    logfile.write(logStr)

