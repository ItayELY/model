from matplotlib import pyplot as plt
import numpy as np
from EDADataFrame import EDADataFrame
from operations.filter import Filter
from operations.group_by import GroupBy
from insights.attribution_insight import AttributionInsight
from insights.contextualization import Contextualize

from miners.outstanding_miner import OutstandingMiner
from miners.reference_mine_mcmc import RefMinerMCMC as RefMiner
# from miners.reference_miner_div_conq import RefMinerDivConq
from miners.trend_miner import TrendMiner
import time

import json
import pandas as pd
import heapq
import warnings
warnings.filterwarnings("ignore")


def by_n_of_attrs(df, ref_miner):
    attrs = list(df.columns)
    time_by_attrs = {}
# for i in range(1, 10):
    for i in range(1, len(attrs)-1):
        start_time = time.time()
        context_insights = ref_miner.mine(overlook_attrs=attrs[i:])
        print(f"---{i} attributes: %s seconds ---" % (time.time() - start_time))
        time_by_attrs[i] = time.time() - start_time
    fig, ax = plt.subplots(1, figsize=(7,7))
    att_nums = list(time_by_attrs.keys())
    times = list(time_by_attrs.values())
    ax.plot(att_nums, times, '-') # this will show date at the x-axis
    ax.set_xticks(att_nums)
    ax.set_xlabel('# of attributes tested')
    ax.set_ylabel('time in seconds')
    ax.set_title('Time of contextualization by # possible of attributes')
    plt.show()


def epochs(df, ref_miner):
    attrs = list(df.columns)
    time_by_attrs = {}
# for i in range(1, 10):
    # for i in range(1, len(attrs)-1):
        # start_time = time.time()
    context_insights = ref_miner.mine()
        # print(f"---{i} attributes: %s seconds ---" % (time.time() - start_time))
        # time_by_attrs[i] = time.time() - start_time
    fig, ax = plt.subplots(1, figsize=(7,7))

    steps = list(ref_miner.log.keys())
    best_scores = list(ref_miner.log.values())
    ax.plot(steps, best_scores, '-') # this will show date at the x-axis
    # ax.set_xticks(steps)
    ax.set_xlabel('# of MCMC steps')
    ax.set_ylabel('Best Score')
    ax.set_title('Best score of insight by # of MCMC steps')
    plt.show()

def by_n_of_rows(df, ref_miner):
    time_by_rows = {}
    attrs = list(df.columns)
    for i in [100, 200, 300, 400, 500, 600, 700]:#, 1000, 1500, 2000, 2500, 3000]:
        ds = EDADataFrame(df.sample(n=i, random_state=1))
# for i in range(1, 10):
        start_time = time.time()
        context_insights = ref_miner.mine(overlook_attrs=attrs[5:])
        print(f"---{i} rows: %s seconds ---" % (time.time() - start_time))
        time_by_rows[i] = time.time() - start_time
    fig, ax = plt.subplots(1, figsize=(7,7))
    row_nums = list(time_by_rows.keys())
    times = list(time_by_rows.values())
    ax.plot(row_nums, times, '-') # this will show date at the x-axis
    ax.set_xticks(row_nums)
    ax.set_xlabel('# of rows tested')
    ax.set_ylabel('time in seconds')
    ax.set_title('Time of contextualization by # of rows')
    plt.show()

def by_attrs(df, ref_miner):
    time_by_rows = {}
    attrs = list(df.columns)
    attrs_lists = [
        ['Gender', 'Income_Category', 'Education_Level', 'Attrition_Flag', 'Card_Category', 'Marital_Status'],
        ['Gender', 'CLIENTNUM', 'Education_Level', 'Attrition_Flag', 'Card_Category', 'Marital_Status'],
        ['Gender', 'CLIENTNUM', 'Credit_Limit', 'Attrition_Flag', 'Card_Category', 'Marital_Status'], 
        ['Credit_Open_To_Buy', 'CLIENTNUM', 'Credit_Limit', 'Attrition_Flag', 'Card_Category', 'Marital_Status'],
        ['Credit_Open_To_Buy', 'CLIENTNUM', 'Credit_Limit', 'Customer_Age', 'Card_Category', 'Marital_Status'],
        # ['Credit_Open_To_Buy', 'CLIENTNUM', 'Credit_Limit', 'Customer_Age', 'Credit_Avg_Utilization_Ratio', 'Marital_Status'],
        # ['Credit_Open_To_Buy', 'CLIENTNUM', 'Credit_Limit', 'Customer_Age', 'Credit_Avg_Utilization_Ratio', 'Months_on_book'],
    ]
    j = 1
    for i in attrs_lists:#, 1000, 1500, 2000, 2500, 3000]:
        # ds = EDADataFrame(df.sample(n=i, random_state=1))
# for i in range(1, 10):
        start_time = time.time()
        context_insights = ref_miner.mine(overlook_attrs=[a for a in attrs if a not in i])
        print(f"---{i} rows: %s seconds ---" % (time.time() - start_time))
        time_by_rows[j] = time.time() - start_time
        j += 1
    fig, ax = plt.subplots(1, figsize=(7,7))
    row_nums = list(time_by_rows.keys())
    times = list(time_by_rows.values())
    ax.plot(row_nums, times, '-') # this will show date at the x-axis
    ax.set_xticks(row_nums)
    ax.set_xlabel('# of diverse attributes tested')
    ax.set_ylabel('time in seconds')
    ax.set_title('Time of contextualization by # of rows')
    plt.show()

def update(x):
    # print(x)
    if x['Income_Category'] in ['Less than $40K', '$40K - $60K']:
        x['Income_Category'] = 'Less than $60K'
    return x

bank_all = pd.read_csv('./bank_churners_user_study.csv')
bank_all = bank_all[bank_all['Income_Category'] != "Unknown"]
bank_all = bank_all.apply(update, axis=1)
bank_all = EDADataFrame(bank_all)

filter1 = Filter('Card_Category', '==', 'Silver')
gb = GroupBy(['Income_Category'], {'Income_Category': 'count'})
# filter1 = Filter('Gender', '==', 'F')
df2 = gb.do_operation(filter1.do_operation(bank_all))
# df2 = filter1.do_operation(bank_all)

miner = OutstandingMiner(df2, df2.columns, None)



insights = miner.mine_top_k(overlook_attrs=['CLIENTNUM'])
insight_1 = insights[0]
# print(list(insights)[0][1][1]) 
ins_objects = [(i[0], i[1]) for i in insights]
# full_cnx_insights = []
ref_miner = RefMiner(miner._df, insight_1)
# ref_miner = RefMinerDivConq(miner._df, insight_1)
contextualized_insights = []
for ins in ins_objects:
    ins_json = ins[1].insight_json()
    # print('*************************************************************')
#     ref_miner = RefMiner(bank_all, ins)
    print(f'Looking at insight with overall score {ins[0]}:\n {json.dumps(ins_json, indent=4)}\n\n')
    
epochs(bank_all, ref_miner)

