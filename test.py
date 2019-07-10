# -*- coding: utf-8 -*-
"""
Created on Wed Jul 10 13:39:30 2019

@author: Yuze Zhou
"""

import pandas as pd 
import time 
tick = pd.read_csv('E:/tick.csv')
tick = tick.loc[:,['time','price','cumv']]
start_time = time.time()
def time_truncate(x):
    if int(str(x)[0]) > 1:
        mid = (str(x))[0:5]
        mid = '0'+mid[0]+':'+mid[1:3]+':'+mid[3:5]
    else:
        mid = (str(x))[0:6]
        mid = mid[0:2]+':'+mid[2:4]+':'+mid[4:6]
    mid = time.strftime("%Y-%m-%d %H:%M:%S",time.strptime(mid, "%H:%M:%S"))
    return mid

tick['time'] = tick['time'].apply(time_truncate)
tick['time'] = pd.to_datetime(tick['time'])
tick = tick.set_index('time')
tick_price_resample = tick.loc[:,'price'].resample('60S',closed='right',label='right')
tick_high = pd.DataFrame(tick_price_resample.max())
tick_high.columns = ['high']
tick_low = pd.DataFrame(tick_price_resample.min())
tick_low.columns = ['low']
tick_open = pd.DataFrame(tick_price_resample.first())
tick_open.columns = ['open']
tick_close = pd.DataFrame(tick_price_resample.last())
tick_close.columns = ['close']
tick_minute = tick_low.join(tick_high)
del(tick_low)
tick_minute = tick_minute.join(tick_open)
del(tick_open)
tick_minute = tick_minute.join(tick_close)
del(tick_close)
end_time = time.time()
print(end_time-start_time)