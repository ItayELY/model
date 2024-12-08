from matplotlib import pyplot as plt
from EDADataFrame import EDADataFrame
from operations.filter import Filter
from operations.group_by import GroupBy
from insights.attribution_insight import AttributionInsight
from insights.contextualization import ContextualizedInsight

from miners.outstanding_miner import OutstandingMiner
from miners.reference_miner import RefMiner
from miners.trend_miner import TrendMiner

from enumerate_filters import EnumFilters

import json
import pandas as pd
import heapq
import warnings
import time


def update(x):
    # print(x)
    if x['Income_Category'] in ['Less than $40K', '$40K - $60K']:
        x['Income_Category'] = 'Less than $60K'
    return x

bank_all = pd.read_csv('./bank_churners_user_study.csv')
bank_all = bank_all[bank_all['Income_Category'] != "Unknown"]
bank_all = bank_all.apply(update, axis=1)
bank_all = EDADataFrame(bank_all)

ef = EnumFilters(bank_all)

filters, cons, diss = ef.generate_filters(depth=1 )
print("filters:")
for f in filters:
    print(f)

print('\n\n\nconjunctions:')
for f in cons:
    print(f)

print('\n\n\ndisjunctions:')
for f in diss:
    print(f)