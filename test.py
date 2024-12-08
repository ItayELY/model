from EDADataFrame import EDADataFrame
from operations.filter import Filter
from operations.group_by import GroupBy
from insights.attribution_insight import AttributionInsight
from insights.contextualization import ContextualizedInsight

import json
import pandas as pd
import warnings
warnings.filterwarnings("ignore")

def update(x):
    # print(x)
    if x['Income_Category'] in ['Less than $40K', '$40K - $60K']:
        x['Income_Category'] = 'Less than $60K'
    return x


bank_all = pd.read_csv('./bank_churners_user_study.csv')
bank_all = bank_all[bank_all['Income_Category'] != "Unknown"]
bank_all = bank_all.apply(update, axis=1)
edf = EDADataFrame(bank_all)
f1 = Filter('Education_Level', '==', 'Uneducated')
# f2 = Filter('Gender', '==', 'F')
print(edf['Total_Transitions_Count'].describe())
# df2 = f1.do_operation(edf)



# gb1 = GroupBy(['Income_Category', 'Gender'], {'Income_Category': 'count'})
# df_gb = gb1.do_operation(bank_all)


# g_attr = 'Income_Category'
# f_attr = 'Gender'
# vals = set(bank_all[f_attr])
# for v in vals:
#     filtered_grouped = df_gb.xs(v, level=f_attr)
#     f = Filter(f_attr, '==', v)
#     print(f"Data for {f_attr} = {v}:")
#     print(filtered_grouped)

# Filter by gender = 'F'
# females = df_gb.xs('F', level='Gender')
# print("\nData for Gender = F:")
# print(females)
# print(df_gb)

# print('\n\n*************Insight*************')
# i1 = AttributionInsight.create_insight_object(df2, None, gb1)
# i1.score()
# # i.show_insight()
# # print(i1.insight_json())
# # print('*************Contextualization*************')
# insight_contx = ContextualizedInsight(insight=i1)
# # print(insight_contx.cnx_json())


# # print('\n\n*************Insight*************')
# i2 = AttributionInsight.create_insight_object(edf, f1, gb1)
# i2.score()
# # i.show_insight()
# # print(i2.insight_json())
# # print('*************Contextualization*************')
# insight_contx2 = ContextualizedInsight(insight=i2)
# # print(insight_contx2.cnx_json())

# cinsight1 = {
#     'target_retreival_query': i1.target_retreival_query,
#     'insight_query': i1.insight_json(),
#     'type': i1.type,
#     'highlight': i1.highlight,
#     'score': i1._score,
#     'contextualization': insight_contx.cnx_json()
# }
# cinsight2 = {
#     'target_retreival_query': i2.target_retreival_query,
#     'insight_query': i2.insight_json(),
#     'type': i2.type,
#     'highlight': i2.highlight,
#     'score': i2._score,
#     'contextualization': insight_contx2.cnx_json()
# }

# cinsights = {
#     'contextualized_insight_1': cinsight1,
#     'contextualized_insight_2': cinsight2,
# }
# json_object = json.dumps(cinsights, indent = 4) 

# # Print JSON object
# print(json_object) 
# with open("./sample.json", "w") as outfile:
#     outfile.write(json_object)