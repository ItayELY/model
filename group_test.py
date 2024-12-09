import json
import operator
# import sys
# import os
# sys.path.append(os.getcwd())
# sys.path.append("..") 
from EDADataFrame import EDADataFrame
# from operations.operation import Operation
from operations.filter import Filter
from operations.group_by import GroupBy
from insights.base_insight import BaseInsight
from miners.outstanding_miner import OutstandingMiner
# from miners.reference_miner import RefMiner
from miners.reference_miner_div_conq import RefMinerDivConq as RefMiner

import matplotlib.pyplot as plt
import pandas as pd

def viz_insight(df, ins, ax, key_title = ''):
    insight_view = ins.get_insight_view(df)


    idx = list(str(i) for i in insight_view.index)
    vals = list(i[0] for i in insight_view.values)
    ax.bar(idx, vals)
        # ax = insight_view.plot.bar(rot=45, figsize=(7,7))
    title = f"{key_title}"
    if ins.filter is not None:
         title += f'- after {str(ins.filter)}'
    if len(ins.target_retreival_query) > 0:
        rq = list(ins.target_retreival_query[0].dict().items())
        title = title + f',\nby \"{rq[0][1]} {rq[1][1]} {rq[2][1]}\"'
    ax.set_title(title)

    ax.set_xlabel(f"{ins.group_by_aggregate.group_attributes[0]}")
    ax.set_ylabel(f"{list(ins.group_by_aggregate.agg_dict.items())[0]}")
    return ax
def update(x):
    # print(x)
    if x['Income_Category'] in ['Less than $40K', '$40K - $60K']:
        x['Income_Category'] = 'Less than $60K'
    return x

bank_all = pd.read_csv('./bank_churners_user_study.csv')
bank_all = bank_all[bank_all['Income_Category'] != "Unknown"]
bank_all = bank_all.apply(update, axis=1)
bank_all = EDADataFrame(bank_all)

gb1 = GroupBy(['Income_Category'], {'Income_Category': 'count'})
# filter1 = Filter('Credit_Limit', 'between', (1000, 2000))
df2 = gb1.do_operation(bank_all)

miner = OutstandingMiner(df2, df2.columns, None)



insights = miner.mine_top_k(overlook_attrs=['CLIENTNUM'])

ref_miner = RefMiner(df2, insights[0])
    # print(f'Looking at insight with overall score {ins[0]}:\n {json.dumps(ins_json, indent=4)}\n\n')
    

     
#     cx_ins = {'obtained_insight': ins_json}
fig, axes = plt.subplots(4, layout = 'constrained', figsize = (7,14))
ind = 1
axes[0] = viz_insight(bank_all, insights[0][1], axes[0], 'Original Insight')
for i in ref_miner.mine().items(): 
        try:
            i_json = i[1][1].insight_json()
            axes[ind] = viz_insight(bank_all, i[1][1], axes[ind], i[0])
            ind += 1
            # print(f'{i[0]}: {json.dumps(i_json, indent = 1, skipkeys = True,) }')
            # cx_ins[f'{i[0]}_insight'] = i_json
        except:
            pass
            # print(f'didnt find interesting {i[0]} ref')
    # contextualized_insights.append(cx_ins)
plt.show()
pass