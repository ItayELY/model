from EDADataFrame import EDADataFrame
from operations.filter import Filter
from operations.group_by import GroupBy
from insights.attribution_insight import AttributionInsight
from insights.contextualization import ContextualizedInsight
from insights.outstanding_insight import OutstandingInsight

import json
import pandas as pd
import warnings
warnings.filterwarnings("ignore")

def update(x):
    # print(x)
    if x['Income_Category'] in ['Less than $40K', '$40K - $60K']:
        x['Income_Category'] = 'Less than $60K'
    return x


adults = EDADataFrame(pd.read_csv('C:/Users/itaye/Desktop/pdexplain/pd-explain/Examples/Datasets/adult.csv'))
gb1 = GroupBy('label', {'label': 'count'})
print('\n\n*************Insight 1*************')
i1 = OutstandingInsight.create_insight_object(adults, None, gb1)
print(i1.score(), i1.target_retreival_query)

f_black = Filter('race', '==', 'Black')
black = f_black.do_operation(adults)
i2 = OutstandingInsight.create_insight_object(black, None, gb1)
print(i2.score(), i2.target_retreival_query)

f_never_married = Filter('marital-status', '==', 'Never-married')
black_never_married = f_never_married.do_operation(black)
i3 = OutstandingInsight.create_insight_object(black_never_married, None, gb1)
print(i3.score(), i3.target_retreival_query)

insight_contx = ContextualizedInsight(insight=i3)
print(insight_contx.cnx_json())
