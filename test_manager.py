import sys


import random
from matplotlib import pyplot as plt
import numpy as np
import pandas as pd
from EDADataFrame import EDADataFrame
from miners.outstanding_miner import OutstandingMiner
from operations.filter import Filter
from operations.group_by import GroupBy
from insights.base_insight import BaseInsight
from insights.outstanding_insight import OutstandingInsight

from final_contextualize.manage_insight_ctx import ContextualizedInsight



all_songs = pd.read_csv('./spotify_all.csv')
all_songs = all_songs.sample(frac=0.1)
all_songs = EDADataFrame(all_songs)

for att in all_songs.columns:
    ser = all_songs[att]
    dtype = all_songs[att].dtype.name
    if (dtype in ['int64', 'float64'] ) and len(ser.value_counts().values) > 10:
        _, bins = pd.cut(ser, 30, retbins=True, duplicates='drop')
        all_songs[f'{att}_binned'] = pd.cut(all_songs[att], bins=bins)

# _, bins = pd.cut(all_songs['popularity'], bins=5, retbins=True)
# all_songs['popularity ']

# attr = 'Dependent_count'
filter1 = Filter('energy', '<', 0.5)
# filter1 = Filter('explicit', '==', 1)



gb = GroupBy(['decade'], {'popularity': 'mean'})
# df2 = filter1.do_operation(all_songs)
df2 =gb.do_operation((filter1.do_operation(all_songs)))

miner = OutstandingMiner(df2, df2.columns, None)



insights = miner.mine_top_k(overlook_attrs=['artists', 'id', 'main_artist', 'name'])
insight_1 = insights[0][1]


manager = ContextualizedInsight(insight_1)
manager.contextualize()
manager.show_ctx_insight()