from matplotlib import pyplot as plt
from EDADataFrame import EDADataFrame
from operations.filter import Filter
from operations.group_by import GroupBy
from insights.attribution_insight import AttributionInsight
from insights.contextualization import Contextualize

from miners.outstanding_miner import OutstandingMiner
# from miners.reference_miner import RefMiner
from miners.reference_miner_div_conq import RefMinerDivConq as RefMiner
from miners.trend_miner import TrendMiner

import json
import pandas as pd
import heapq
import warnings
import time

warnings.filterwarnings("ignore")

def viz_insight(df, ins, ax, key_title = ''):
    insight_view = ins.get_insight_view(df)


    idx = list(str(i) for i in insight_view.index)
    vals = list(i[0] for i in insight_view.values)
    ax.bar(idx, vals)
        # ax = insight_view.plot.bar(rot=45, figsize=(7,7))
    title = f"{key_title} when filtering"
    flag = False
    if len(ins.target_retreival_query) > 0:
        for r in ins.target_retreival_query:
            rq = list(r.dict().items())
            title = title + f'\nmanually by \"{rq[0][1]} {rq[1][1]} {rq[2][1]}\"'
            flag = True
    if ins.filter is not None:
        if flag:
             title += ', followed'
        title = title + f'\nby insight-driven filter \"{ins.filter.attribute} {ins.filter.operation_str} {ins.filter.value}\"'
    
    ax.set_title(title)
    
    ax.set_xlabel(f"{ins.group_by_aggregate.group_attributes[0]}")
    agg_item = list(ins.group_by_aggregate.agg_dict.items())[0]
    ax.set_ylabel(f"{agg_item[1]} of {agg_item[0]}")

    ax.tick_params(axis='x', labelrotation=50)
    return ax

# def update(x):
#     # print(x)
#     if x['Income_Category'] in ['Less than $40K', '$40K - $60K']:
#         x['Income_Category'] = 'Less than $60K'
#     return x

bank_all = pd.read_csv('./spotify_all.csv')
# bank_all = bank_all[bank_all['Income_Category'] != "Unknown"]
# bank_all = bank_all.apply(update, axis=1)
bank_all = EDADataFrame(bank_all)

# filter1 = Filter('Education_Level', '==', 'Uneducated')
filter1 = Filter('popularity', '>=', 65)
df2 = filter1.do_operation(bank_all)
filter2 = Filter('loudness', '<=', -20.5)
df23 = filter2.do_operation(df2)
gb = GroupBy(['decade'], {'acousticness': 'mean'})
df_gb = gb.do_operation(df2)
df_t = filter1.do_operation(bank_all)
# df2 = bank_all

miner = OutstandingMiner(df_t, df_t.columns, None)
# miner = OutstandingMiner(df_gb, df2.columns, None)

# overlook_attrs=['id', 'liveness', 'energy', 'danceability', 'speechiness', 'instrumentalness', 'year', 'popularity_score', 'duration_minutes']
overlook_attrs = ['id', 'popularity_score', 'year', 'acousticness']

start_time = time.time()
insights = miner.mine_top_k(overlook_attrs=overlook_attrs)
print("Insight Mining--- %s seconds ---" % (time.time() - start_time))

insight_1 = insights[0]
# print(list(insights)[0][1][1]) 
ins_objects = [(i[0], i[1]) for i in insights]
# full_cnx_insights = []
ref_miner = RefMiner(miner._df, insight_1)
contextualized_insights = []
# for ins in ins_objects:
    # ins_json = ins[1].insight_json()
    # print('*************************************************************')
#     ref_miner = RefMiner(bank_all, ins)
    # print(f'Looking at insight with overall score {ins[0]}:\n {json.dumps(ins_json, indent=4)}\n\n')
    


# plt.xticks(rotation=50)
start_time = time.time()
contx = ref_miner.mine(overlook_attrs = overlook_attrs, level=1).items()
print("Reference Mining--- %s seconds ---" % (time.time() - start_time))

fig, axes = plt.subplots(len(contx), layout = 'constrained', figsize = (7,7 + 7 * len(contx)))
fig.suptitle('Contextualized Insights-\nInitial Data taken from \"songs_df\"')
ind = 1
axes[0] = viz_insight(df_t, insight_1[1], axes[0], 'Obtained Insight- outstanding category')
for i in contx: 
        try:
            i_json = i[1][1].insight_json()
            axes[ind] = viz_insight(bank_all, i[1][1], axes[ind], f'{i[0]} effect is given')
            ind += 1
            # print(f'{i[0]}: {json.dumps(i_json, indent = 1, skipkeys = True,) }')
            # cx_ins[f'{i[0]}_insight'] = i_json
        except:
            pass
            # print(f'didnt find interesting {i[0]} ref')
    # contextualized_insights.append(cx_ins)
plt.show()


#     cx_ins = {'obtained_insight': ins_json}
# for i in ref_miner.mine().items(): 
#         try:
#             i_json = i[1][1].insight_json()
#             # print(f'{i[0]}: {json.dumps(i_json, indent = 1, skipkeys = True,) }')
#             cx_ins[f'{i[0]}_insight'] = i_json
#         except:
#             pass
#             # print(f'didnt find interesting {i[0]} ref')
#     contextualized_insights.append(cx_ins)
    
# json_object = json.dumps(contextualized_insights, indent = 4) 

# # Print JSON object
# print(f'{json_object}') 

# with open("./miner_output.json", "w") as outfile:
#     outfile.write(json_object)
# '''
pass