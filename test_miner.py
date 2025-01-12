from matplotlib import pyplot as plt
from EDADataFrame import EDADataFrame
from operations.filter import Filter
from operations.group_by import GroupBy
from insights.attribution_insight import AttributionInsight

from miners.outstanding_miner import OutstandingMiner
# from miners.reference_miner import RefMiner
from miners.reference_miner_div_conq import RefMinerDivConq
from miners.reference_miner_div_conq_sample import RefMinerDivConqSample
from miners.reference_mine_mcmc import RefMinerMCMC

from miners.trend_miner import TrendMiner
import time

import json
import pandas as pd
import heapq
import warnings
warnings.filterwarnings("ignore")

def viz_insight(df, ins, ax, key_title = '', highlight = ''):
    insight_view = ins.get_insight_view(df)


    idx = list(str(i) for i in insight_view.index)
    vals = list(i[0] for i in insight_view.values)
    colors = []
    for i in idx:
        if i == str(highlight):
            colors.append('orange')
        else:
            colors.append('blue')
    # colors = ['blue' for i in idx if i != str(i.highlight) else 'orange']  # Highlight the second bar

    ax.bar(idx, vals, color=colors)
        # ax = insight_view.plot.bar(rot=45, figsize=(7,7))
    title = f"{key_title} when the input data is filtered\n"
    flag = False
    if len(ins.target_retreival_query) > 0:
        for r in ins.target_retreival_query:
            rq = list(r.dict().items())
            title = title + f'\nmanually by \"{rq[0][1]} {rq[1][1]} {rq[2][1]}\"\n'
            flag = True
    if ins.filter is not None and str(ins.filter) != '':
        if flag:
             title += 'followed '

        title = title + f'by a suggested filter {str(ins.filter)}'
    
    ax.set_title(title)
    
    ax.set_xlabel(f"{ins.group_by_aggregate.group_attributes[0]}")
    agg_item = list(ins.group_by_aggregate.agg_dict.items())[0]
    ax.set_ylabel(f"{agg_item[1]} of {agg_item[0]}")

    ax.tick_params(axis='x', labelrotation=50)
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

# filter1 = Filter('Credit_Limit', 'between', (12000, 23000))
filter1 = Filter('Gender', '==', 'M')
# df2 = filter1.do_operation(bank_all)
# df2 = filter1.do_operation(bank_all)

gb = GroupBy(['Income_Category'], {'Income_Category': 'count'})
# filter1 = Filter('Gender', '==', 'F')
df2 = gb.do_operation(filter1.do_operation(bank_all))
# df2 = filter1.do_operation(bank_all)

miner = OutstandingMiner(df2, df2.columns, None)



insights = miner.mine_top_k(overlook_attrs=['CLIENTNUM'])
insight_1 = insights[0]
# insight_1 = insights[1]
# print(list(insights)[0][1][1]) 
ins_objects = [(i[0], i[1]) for i in insights]
# full_cnx_insights = []
# ref_miner = RefMinerDivConq(miner._df, insight_1)
ref_miner = RefMinerMCMC(miner._df, insight_1)
contextualized_insights = []
for ins in ins_objects:
    ins_json = ins[1].insight_json()
    # print('*************************************************************')
#     ref_miner = RefMiner(bank_all, ins)
    print(f'Looking at insight with overall score {ins[0]}:\n {json.dumps(ins_json, indent=4)}\n\n')
    

start_time = time.time()
context_insights = ref_miner.mine(overlook_attrs=['Credit_Open_To_Buy', 'CLIENTNUM'])
fig, axes = plt.subplots(2, 2, layout = 'constrained', figsize = (14,14))

ind = 1
ind2 = 0
hl = insight_1[1].highlight
axes[0][0] = viz_insight(df2.prev_df, insight_1[1], axes[0][0], 'Outstanding category is detected', hl)
print("--- %s seconds ---" % (time.time() - start_time))

for i in context_insights.items(): 
        try:
            # i_json = i[1][1].insight_json()
            axes[ind2][ind] = viz_insight(bank_all, i[1][0][1], axes[ind2][ind], i[0]+' effect', hl)
            ind += 1
            if ind == 2:
                ind = 0
                ind2 += 1
            # print(f'{i[0]}: {json.dumps(i_json, indent = 1, skipkeys = True,) }')
            # cx_ins[f'{i[0]}_insight'] = i_json
        except Exception as e:
# 
            pass
            print(f'didnt find interesting {i[0]} ref')
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