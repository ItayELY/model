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

from final_contextualize.global_contextualize import GlobalContextualize




def get_df_name(df):
    name =[x for x in globals() if globals()[x] is df][0]
    return name
def viz_insight_with_ref(df, df_ref, ins, ax, key_title = '', highlight = ''):
    
    insight_view = ins.get_insight_view(df)
    # insight_ref = ins.create_insight_object(df_ref, ins.filter, ins.group_by_aggregate)
    insight_view_ref = ins.get_insight_view(df_ref)
    # idx = set(list(str(i) for i in insight_view.index) + list(str(i) for i in insight_view_ref.index))
    idx = set(list(i for i in insight_view.index) + list(i for i in insight_view_ref.index))

    for i in idx:
        if i not in list((i) for i in insight_view.index):
            insight_view.loc[i] = 0
        if i not in list((i) for i in insight_view_ref.index):
            insight_view_ref.loc[i] = 0
    
    vals = list(insight_view.loc[i][0] for i in idx)
    vals_ref = list(insight_view_ref.loc[i][0] for i in idx)
    colors = []
    colors_ref = []
    for i in idx:
        if i == str(highlight):
            colors.append('blue')
            colors_ref.append('orange')
        else:
            colors.append('cornflowerblue')
            colors_ref.append('sandybrown')
    # colors = ['blue' for i in idx if i != str(i.highlight) else 'orange']  # Highlight the second bar
    X_axis = np.arange(len(idx))
    ax.bar(X_axis - 0.2, vals, 0.4, color=colors, align="center")
    ax.bar(X_axis + 0.2, vals_ref, 0.4, color=colors_ref, align="center")
    title = f'{key_title}:\nYour df (blue)- {df.get_path()}\nreference df (orange)- {df_ref.get_path()}'

    
    
    

    ax.set_title(title)
    
    ax.set_xlabel(f"{ins.group_by_aggregate.group_attributes[0]}")
    agg_item = list(ins.group_by_aggregate.agg_dict.items())[0]
    ax.set_ylabel(f"{agg_item[1]} of {agg_item[0]}")
    ticklabels = [str(i) for i in idx]
    ax.set_xticks(X_axis)

    ax.set_xticklabels(ticklabels)
    ax.tick_params(axis='x', labelrotation=50)
    return ax

def update(x):
    # print(x)
    if x['Income_Category'] in ['Less than $40K', '$40K - $60K']:
        x['Income_Category'] = 'Less than $60K'
    return x

all_songs = pd.read_csv('./spotify_all.csv')
all_songs = all_songs.sample(frac=0.1)
all_songs = EDADataFrame(all_songs)

for att in all_songs.columns:
    ser = all_songs[att]
    dtype = all_songs[att].dtype.name
    if (dtype in ['int64', 'float64'] ) and len(ser.value_counts().values) > 10:
        _, bins = pd.cut(ser, 5, retbins=True, duplicates='drop')
        all_songs[f'{att}_binned'] = pd.cut(all_songs[att], bins=bins)

# _, bins = pd.cut(all_songs['popularity'], bins=5, retbins=True)
# all_songs['popularity ']

# attr = 'Dependent_count'
filter2 = Filter('explicit', '==', 1)
filter1 = Filter('year', '>', 1990)

# ser = set(bank_all[attr])

# val = filter1.value

# other_filters = [Filter(attr, '==', v) if v != val else None for v in ser]
# other_filters = [Filter(attr, '!=', 'Uneducated')]



gb = GroupBy(['decade'], {'popularity': 'mean'})
df2 = filter1.do_operation(all_songs)
# df2 = filter2.do_operation((filter1.do_operation(all_songs)))

miner = OutstandingMiner(df2, df2.columns, None)



insights = miner.mine_top_k(overlook_attrs=['artists', 'id', 'main_artist', 'name'])
insight_1 = insights[2]
print(f'insight: {(insight_1[1].show_insight())}')
ctx = GlobalContextualize(df2, insight_1[1])
# ctx.get_neighbors(1)
similar, dist = ctx.contextualize(1)
fig, ax = plt.subplots(2, layout = 'constrained', figsize = (7,14))

if len(ctx.sim_insights) > 0:
    sim_insight = ctx.sim_insights[0]
    ax[0] = viz_insight_with_ref(df2, sim_insight[0]._df, insight_1[1], ax[0], 'similar', insight_1[1].highlight)

if len(ctx.dist_insights) > 0:
    dist_insight = ctx.dist_insights[0]
    ax[1] = viz_insight_with_ref(df2, dist_insight[0]._df, insight_1[1], ax[1], 'distinct', insight_1[1].highlight)


ind = 0
ind2 = 0
dist_list = []
dist_tuples = []
similar_list = []






# for score, df in similar:
#     if ind == 3:
#         ind = 0
#         ind2 += 1
#     similar_list.append(df.get_path() + f': {score}')
#     # viz_insight_with_ref(df2.prev_df, df.prev_df, insight_1[1], ax[ind][ind2], 'similar', insight_1[1].highlight)
#     ind += 1
# for score, df in dist:
#     if ind == 3:
#         ind = 0
#         ind2 += 1
#     dist_list.append(df.get_path() + f': {score}')
#     # viz_insight_with_ref(df2.prev_df, df.prev_df, insight_1[1], ax[ind][ind2], 'dist', insight_1[1].highlight)
#     ind += 1
# # print('similar:')
# for s in ctx.sim_ranges:
#     # print(f'\t{s}')
#     pass
# # print('dist:')

# for d in ctx.dist_ranges:
#     # print(f'\t{d}')
#     pass
plt.show()
pass
     
