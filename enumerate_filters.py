import heapq
import sys
# sys.path.append("..") 

from EDADataFrame import EDADataFrame
from operations.filter import Filter
from operations.filters_con import FiltersCon
from operations.filters_dis import FiltersDis
from operations.group_by import GroupBy
from insights.base_insight import BaseInsight

import json
import pandas as pd
import warnings
warnings.filterwarnings("ignore")

from itertools import product

# Example attributes, operators, and values
# attributes = ["age", "income", "country"]
operators = ["==", "!="]
# values = [30, 50000, "'USA'", "'Canada'"]

class EnumFilters():
    def __init__(self, df: EDADataFrame, overlook_attrs = [], n_bins_f = 10):
        self._df = df
        self.attributes = []
        self.attributes_int = []
        for f_attr in self._df.columns:
                if f_attr in overlook_attrs:
                    continue
                # if f_attr == 'Credit_Limit':
                    # pass
                # if 'binned' in f_attr:
                    # continue
                # if f_attr == g_attr:# and df[f_attr].dtype.name not in ['int64', 'float64']):
                    # continue
                dtype = df[f_attr].dtype.name
                if len(df[f_attr].value_counts().values) > 10:
                    if not (dtype in ['int64', 'float64'] ) :
                        continue
                    _, bins = pd.cut(df[f_attr], n_bins_f, retbins=True, duplicates='drop')
                    self._df[f'{f_attr}'] = pd.cut(df[f_attr], bins=bins)
                    self.attributes_int.append(f_attr)
                else:
                    self.attributes.append(f_attr)
                    # fattr = f'{f_attr}'

                # i = self.filter_mine(g_attr, f_attr, df, gb, k, size, [])
                # if i is None:
                    # continue
                # for i in insights:
                # if len(self._insights) < k:
                    # heapq.heappush(self._insights, (i[0], i[1]))
                # else:
                    # spilled_value = heapq.heappushpop(self._insights, (i[0], i[1]))
        
        self.values = {col: self._df[col].unique().tolist() for col in self.attributes}
        self.values_int = {col: self._df[col].unique().tolist() for col in self.attributes_int}
        # print("Values by Attribute:", self.values_int)



# Recursive function to generate filters
    def generate_filters(self, depth = 1):
        if depth == 0:
            return []
    
#     # Generate basic conditions: attr op val
        basic_conditions_1 = []
        basic_conditions_2 = []
        filters = []
        for k, v in self.values.items():
            for val in v:
                basic_conditions_1.append(f"{k} == {val}")
                
                filters.append(Filter(k, '==', val))
        for k, v in self.values_int.items():
            for val in v:
                basic_conditions_1.append(f"{k} between {val}")
                
                filters.append(Filter(k, 'between', (val.left, val.right)))
        if depth == 1:
            return filters, [], []
        # for k, v in self.values_int.items():
            # basic_conditions_2 += [
                # f"{k} between ({val.left}, {val.right})" for val in v
            # ]

    # Combine conditions using 'and' or 'or'
        # combined_conditions = [
        #     f"({c1} {logical} {c2})"
        #     for c1, c2 in product(basic_conditions_1, repeat=2)
        #     for logical in ["and", "or"]
        # ]
        con_filters = []
        dis_filters = []
        for f1, f2 in product(filters, repeat=2):
            con_filters.append(FiltersCon([f1, f2]))
            dis_filters.append(FiltersDis([f1, f2]))
        # combined_conditions = [
        #     f"({c1} {logical} {c2})"
        #     for c1, c2 in product(basic_conditions_1, repeat=2)
        #     for logical in ["and", "or"]
        # ]
        return filters, con_filters, dis_filters

    
#     # Include deeper combinations recursively
#     deeper_conditions = generate_filters(attributes, operators, values, depth - 1)
#     return basic_conditions + combined_conditions + deeper_conditions

# # Generate all filters up to depth 2
# filters = generate_filters(attributes, operators, values, depth=2)
# for f in filters[:10]:  # Print the first 10 filters
#     print(f)
    def generate_filters_by_attr(self, depth = 1):
        if depth == 0:
            return []
    
#     # Generate basic conditions: attr op val
        basic_conditions_1 = []
        basic_conditions_2 = []
        filter_dict = {}
        for k, v in self.values.items():
            filter_dict[k] = []
            for val in v:
                basic_conditions_1.append(f"{k} == {val}")
                
                filter_dict[k].append(Filter(k, '==', val))
        for k, v in self.values_int.items():
            for val in v:
                basic_conditions_1.append(f"{k} between {val}")
                
                filter_dict[k].append(Filter(k, 'between', (val.left, val.right)))
        # if depth == 1:
        return filter_dict, [], []
        # for k, v in self.values_int.items():
            # basic_conditions_2 += [
                # f"{k} between ({val.left}, {val.right})" for val in v
            # ]

    # Combine conditions using 'and' or 'or'
        # combined_conditions = [
        #     f"({c1} {logical} {c2})"
        #     for c1, c2 in product(basic_conditions_1, repeat=2)
        #     for logical in ["and", "or"]
        # ]
        # con_filters = []
        # dis_filters = []
        # for f1, f2 in product(filters, repeat=2):
        #     con_filters.append(FiltersCon([f1, f2]))
        #     dis_filters.append(FiltersDis([f1, f2]))
        # # combined_conditions = [
        # #     f"({c1} {logical} {c2})"
        # #     for c1, c2 in product(basic_conditions_1, repeat=2)
        # #     for logical in ["and", "or"]
        # # ]
        # return filters, con_filters, dis_filters