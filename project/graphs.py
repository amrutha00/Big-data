#!/usr/bin/env python
# coding: utf-8

# In[3]:


import pandas as pd
import json
import time
from datetime import datetime as d
import matplotlib.pylab as plt

file_path=['worker1.log','worker2.log','worker3.log']


color=['b','g','r','c','m','y','k','w']
worker=[]
for path in file_path: 

    data=open(path,'r')
    logs={}

    for line in data:
        col  = line.split(';')
        a=col[-1].strip()
        x=d.strptime(a,"%Y-%m-%d %H:%M:%S,%f")
        if(x not in logs.keys()):
            logs[x]=[]
        record = [col[2]] 
        logs[x].append(record)
    
    final_list={}
    curr=0
    for i in logs.keys():
        x=logs[i]
        for j in x:
            col=j[0]
            col = col.split(' ')
            if(col[0]=='Started'):
                curr=curr+1
            else:
                curr= curr-1
        final_list[i]=curr
    worker.append(final_list)

plt.figure(figsize=(8,8))
for i in range(0,len(file_path)):
    x='worker' +str(i+1)
    plt.plot(*zip(*sorted(worker[i].items())),marker='o',color= color[i],label=x)
plt.xlabel('Time')
plt.ylabel('Running Tasks')
plt.legend()
plt.show()    

