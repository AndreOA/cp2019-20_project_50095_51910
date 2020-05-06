#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd 
import matplotlib.pyplot as plt
import numpy as np



# Read data from file 'filename.csv' 
# (in the same directory that your python process is based)
# Control delimiters, rows, column names with read_csv (see later) 
par_data_6 = pd.read_csv("output_t6_par.csv") 
par_data_6.columns = ['threads', 'size', 'test', 'time']
seq_data_6 = pd.read_csv("output_t6_seq.csv") 
seq_data_6.columns = ['threads', 'size', 'test', 'time']

s = [seq_data_6['time'].values[0],seq_data_6['time'].values[0],seq_data_6['time'].values[0],seq_data_6['time'].values[0],seq_data_6['time'].values[0],seq_data_6['time'].values[0],seq_data_6['time'].values[0]]

plt.figure(1, figsize=(12,8))
plt.plot(par_data_6['threads'], s/par_data_6['time'].values, '-ro', label="Reduce Heavy Worker")
plt.ylim(bottom=0)
plt.xlabel("Number of Threads")
plt.ylabel("SpeedUp")
plt.legend()
plt.title("Time Speedup for different number of Threads with Input = 2000000")
plt.show()
#plt.savefig('plots/test2_speedup.png')
plt.close()


