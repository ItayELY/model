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
    idx = set(list(str(i) for i in insight_view.index) + list(str(i) for i in insight_view_ref.index))
    for i in idx:
        if i not in list(str(i) for i in insight_view.index):
            insight_view.loc[i] = 0
        if i not in list(str(i) for i in insight_view_ref.index):
            insight_view_ref.loc[i] = 0



    # sc_similar = ctx.similar_score()
    # sc_dist = ctx.distinction_score()
    # if sc_similar > sc_dist:
    #     label = 'similarity detected'
    # else:
    #     label = 'distinction detected'

    # idx = list(str(i) for i in insight_view.index)
    
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

bank_all = pd.read_csv('./bank_churners_user_study.csv')
# bank_all = bank_all[bank_all['Income_Category'] != "Unknown"]
# bank_all = bank_all.apply(update, axis=1)
bank_all = EDADataFrame(bank_all)

attr = 'Dependent_count'
filter1 = Filter(attr, '==', 3)
filter2 = Filter('Education_Level', '==', 'Uneducated')

ser = set(bank_all[attr])

val = filter1.value

# other_filters = [Filter(attr, '==', v) if v != val else None for v in ser]
other_filters = [Filter(attr, '!=', 'Uneducated')]



gb = GroupBy(['Income_Category'], {'Credit_Limit': 'mean'})
df1 = filter1.do_operation(bank_all)
df2 = gb.do_operation(filter2.do_operation(filter1.do_operation(bank_all)))

miner = OutstandingMiner(df2, df2.columns, None)



insights = miner.mine_top_k(overlook_attrs=['CLIENTNUM'])
insight_1 = insights[0]
ctx = GlobalContextualize(df2, insight_1[1])
ctx.get_neighbors(2)
similar, dist = ctx.contextualize(2)

fig, ax = plt.subplots(3, 2, layout = 'constrained', figsize = (14,14))
ind = 0
ind2 = 0
for score, df in similar:
    if ind == 3:
        ind = 0
        ind2 += 1
    viz_insight_with_ref(df2.prev_df, df.prev_df, insight_1[1], ax[ind][ind2], 'similar', insight_1[1].highlight)
    ind += 1
for score, df in dist:
    if ind == 3:
        ind = 0
        ind2 += 1
    viz_insight_with_ref(df2.prev_df, df.prev_df, insight_1[1], ax[ind][ind2], 'dist', insight_1[1].highlight)
    ind += 1
plt.show()
pass
     
