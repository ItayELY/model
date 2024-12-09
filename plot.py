from matplotlib import pyplot as plt
from EDADataFrame import EDADataFrame
from operations.filter import Filter
from operations.group_by import GroupBy
from insights.attribution_insight import AttributionInsight
from insights.contextualization import Contextualize

from miners.outstanding_miner import OutstandingMiner
from miners.reference_miner import RefMiner
from miners.trend_miner import TrendMiner
import time

import json
import pandas as pd
import heapq
import warnings
warnings.filterwarnings("ignore")

time_by_attrs = {'1':0.26957273483276367, '2': 2.555959701538086, '3': 3.704549551010132, '4':  10.1, '4': 3.704549551010132, '4': 16.057, '6': 17.684, '7': 22.530, '8': 22.5, '9': 27.8, '10': 32.1, '11': 56.7, '12': 73.78, '13': 95.3}
fig, ax = plt.subplots(1, figsize=(7,7))
att_nums = list(time_by_attrs.keys())
times = list(time_by_attrs.values())
ax.plot(att_nums, times, '-') # this will show date at the x-axis
ax.set_xticks(att_nums)
ax.set_xlabel('# of attributes tested')
ax.set_ylabel('time in seconds')
ax.set_title('Time of contextualization by # possible of attributes')
plt.show()