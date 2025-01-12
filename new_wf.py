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
from insights.contextualization import Contextualize

def get_df_name(df):
    name =[x for x in globals() if globals()[x] is df][0]
    return name

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
    title = f"original dataframe: {key_title} when the input data is filtered\n"
    flag = False
    
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


def viz_insight_with_ref(df, df_ref, ins, ax, key_title = '', highlight = ''):
    insight_view = ins.get_insight_view(df)
    insight_ref = ins.create_insight_object(df_ref, ins.filter, ins.group_by_aggregate)
    insight_view_ref = ins.get_insight_view(df_ref)

    ctx = Contextualize(bank_all, ins, insight_ref)
    sc_similar = ctx.similar_score()
    sc_dist = ctx.distinction_score()
    if sc_similar > sc_dist:
        label = 'similarity detected'
    else:
        label = 'distinction detected'

    idx = list(str(i) for i in insight_view.index)
    vals = list(i[0] for i in insight_view.values)
    vals_ref = list(i[0] for i in insight_view_ref.values)
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
    ax.bar(X_axis - 0.2, vals, 0.4, color=colors)
    ax.bar(X_axis + 0.2, vals_ref, 0.4, color=colors_ref)
        # ax = insight_view.plot.bar(rot=45, figsize=(7,7))
    title = f"Contextualization: {label}\n"
    title += f"acress the target df: {get_df_name(df)}(blue)\n"
    title += f"and the reference dataframe: {get_df_name(df_ref)}(orange)\n"

    title += key_title

    # flag = False
    # if len(ins.target_retreival_query) > 0:
    #     for r in ins.target_retreival_query:
    #         rq = list(r.dict().items())
    #         title = title + f'manually by \"{rq[0][1]} {rq[1][1]} {rq[2][1]}\"\n'
    #         flag = True
    # if ins.filter is not None and str(ins.filter) != '':
    #     if flag:
    #          title += 'followed '

    #     title = title + f'by a suggested filter {str(ins.filter)}\n'
    
    
    

    ax.set_title(title)
    
    ax.set_xlabel(f"{ins.group_by_aggregate.group_attributes[0]}")
    agg_item = list(ins.group_by_aggregate.agg_dict.items())[0]
    ax.set_ylabel(f"{agg_item[1]} of {agg_item[0]}")
    ax.set_xticklabels([str(i) for i in idx])
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

for att in bank_all.columns:
    ser = bank_all[att]
    dtype = bank_all[att].dtype.name
    if (dtype in ['int64', 'float64'] ) and len(ser.value_counts().values) > 10:
        _, bins = pd.cut(ser, 5, retbins=True, duplicates='drop')
        bank_all[f'{att}_binned'] = pd.cut(bank_all[att], bins=bins)
                # g_attr_new = f'{att}_binned'

attr = 'Education_Level'
# filter_uneducated = Filter(attr, '==', 'Uneducated')

filter_college = Filter('Income_Category', '==', '$120K +')
other_filters = [Filter('Income_Category', '==', '$60K - $80K'), Filter('Income_Category', '==', '$80K - $120K'). Filter('Income_Category', '==', 'Less than $60K')]
# uneducated =filter_uneducated.do_operation(bank_all)
college = filter_college.do_operation(bank_all)

miner = OutstandingMiner(college, college.columns, None)
insights = miner.mine_top_k(overlook_attrs=['CLIENTNUM'], k=4)
fig, axes = plt.subplots(2, 2, layout = 'constrained', figsize = (14,14))

ind1 = 0
ind2 = 0

for i in insights:
    if ind1 == 2:
        ind2 += 1
        ind1 = 0
    hl = i[1].highlight
    axes[ind1][ind2] = viz_insight_with_ref(uneducated, college, i[1], axes[ind1][ind2], f'{str(i[1].highlight)} is an Outstanding category', hl)
    # print("--- %s seconds ---" % (time.time() - start_time))
    ind1 += 1
plt.show()
pass