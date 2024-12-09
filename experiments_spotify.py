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

all_songs = pd.read_csv('./spotify_all.csv')
decade_vals = all_songs['year'].value_counts()
songs_1 = all_songs[all_songs['year'] <= 1930]
s1 = songs_1.shape[0]
songs_2 = all_songs[all_songs['year'] <= 1940]
s2 = songs_2.shape[0]
songs_3 = all_songs[all_songs['year'] <= 1949]
s3 = songs_3.shape[0]
songs_4 = all_songs[all_songs['year'] <= 1955]
s4 = songs_4.shape[0]
songs_5 = all_songs[all_songs['year'] <= 1962]
s5 = songs_5.shape[0]

songs_6 = all_songs[all_songs['year'] <= 1968]
s6 = songs_6.shape[0]
songs_7 = all_songs[all_songs['year'] <= 1975]
s7 = songs_7.shape[0]
# songs_8 = all_songs[all_songs['year'] <= 1954]
# s8 = songs_8.shape[0]
# songs_9 = all_songs[all_songs['year'] <= 1956]
# s9 = songs_9.shape[0]
# songs_10 = all_songs[all_songs['year'] <= 1958]
# s10 = songs_10.shape[0]

# songs_11 = all_songs[all_songs['year'] <= 1960]
# s11 = songs_11.shape[0]
# songs_12 = all_songs[all_songs['year'] <= 1962]
# s12 = songs_12.shape[0]
# songs_13 = all_songs[all_songs['year'] <= 1964]
# s13 = songs_13.shape[0]
# songs_14 = all_songs[all_songs['year'] <= 1966]
# s14 = songs_14.shape[0]
# songs_10 = all_songs[all_songs['year'] <= 1968]
# s10 = songs_10.shape[0]
songs = EDADataFrame(all_songs)
datasets = [songs_1, songs_2, songs_3, songs_4, songs_5, songs_6, songs_7]#, songs_8, songs_9, songs_10, songs_11, songs_12, songs_13, songs_14]
datasets = [EDADataFrame(d) for d in datasets]

# new_songs =all_songs[all_songs['decade'] >= 1980]
# songs = EDADataFrame(new_songs)

filter1 = Filter('popularity', '>=', 20)
size_to_time = {}
# for i in range (5000, 50000, 3000):
    # ds = EDADataFrame(songs.sample(n=i, random_state=1))
for ds in datasets:
    df2 = filter1.do_operation(ds)
# filter2 = Filter('loudness', '<=', -20.5)
# df23 = filter2.do_operation(df2)
    gb = GroupBy(['decade'], {'acousticness': 'mean'})
    df_gb = gb.do_operation(df2)
# df_t = filter2.do_operation(bank_all)
# df2 = bank_all

    miner = OutstandingMiner(df_gb, ds.columns, None)
# miner = OutstandingMiner(df_gb, df2.columns, None)

# overlook_attrs=['id', 'liveness', 'energy', 'danceability', 'speechiness', 'instrumentalness', 'year', 'popularity_score', 'duration_minutes']
    overlook_attrs = ['id', 'popularity_score', 'year', 'acousticness']

    # start_time = time.time()
    insights = miner.mine_top_k(overlook_attrs=overlook_attrs)
    # print("Insight Mining--- %s seconds ---" % (time.time() - start_time))

    insight_1 = insights[0]
# print(list(insights)[0][1][1]) 
    # ins_objects = [(j[0], i[1]) for i in insights]
    ref_miner = RefMiner(miner._df, insight_1)
    contextualized_insights = []

    start_time = time.time()
    contx = ref_miner.mine(overlook_attrs = overlook_attrs, level=1).items()
    t = time.time() - start_time
    print("Reference Mining--- %s seconds ---" % (t))
    size_to_time[ds['year'].max()] = t
    
fig, ax = plt.subplots(1, figsize=(7,7))
att_nums = list(size_to_time.keys())
times = list(size_to_time.values())
ax.plot(att_nums, times, '-') # this will show date at the x-axis
ax.set_xticks(att_nums)
ax.set_xlabel('highest year limit')
ax.set_ylabel('time in seconds')
ax.set_title('Time of contextualization by range of year values')

# attrs = list(songs.columns)
# time_by_attrs = {} 
# for i in range(1, len(attrs)-1):
#     start_time = time.time()
#     context_insights = ref_miner.mine(overlook_attrs=attrs[i:])
#     print(f"---{i} attributes: %s seconds ---" % (time.time() - start_time))
#     time_by_attrs[i] = time.time() - start_time

plt.show()
