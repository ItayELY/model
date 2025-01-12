import heapq
import sys

sys.path.append("..") 

from EDADataFrame import EDADataFrame
from operations.filter import Filter
from operations.multi_filter import MultiFilter
from operations.group_by import GroupBy
from insights.attribution_insight import AttributionInsight
from insights.outstanding_insight import OutstandingInsight
from insights.base_insight import BaseInsight
from insights.trend_insight import TrendInsight

import matplotlib.pyplot as plt
import json
import pandas as pd
import warnings
warnings.filterwarnings("ignore")
SIG = 0.9
IMP = 1 - SIG

class Miner():
    def __init__(self, df: EDADataFrame, group_attrs: list | None, agg_attrs: list | None) -> None:
        self._df = df
        self._group_attrs = group_attrs
        self._agg_attrs = agg_attrs
        self.capacity = 5
        self.pairs = []
    
    def mine(self):
        insights = []
        top_insight_per_filter = {}
        top_insight_per_filter['No'] = (0, None)
        for g_attr in self._group_attrs:
            if  g_attr == 'Card_Category':
                continue
            max_score = 0
            df = self._df.copy()
            size = df.shape[0]
            dtype = df[g_attr].dtype.name
            n_bins = 5
            g_attr_new = g_attr
            if dtype in ['int64', 'float64']:
                _, bins = pd.cut(df[g_attr], n_bins, retbins=True, duplicates='drop')
                df[f'{g_attr}_binned'] = pd.cut(df[g_attr], bins=bins)
                g_attr_new = f'{g_attr}_binned'
            gb = GroupBy([g_attr_new], {g_attr_new: 'count'})
            # i = OutstandingInsight.create_insight_object(df, None, gb)
            i = TrendInsight.create_insight_object(df, None, gb)
            max_score = i.score()

            if max_score > top_insight_per_filter['No'][0]:
                top_insight_per_filter['No'] = (max_score, i)
            # attr_insight = i.insight_json()
            heapq.heappushpop(insights, (max_score, i))
            if(len(insights) == 6):
                pass
            for f_attr in df.columns:
                if 'binned' in f_attr:
                    continue
                if not (f_attr != g_attr and df[f_attr].dtype.name not in ['int64', 'float64']):
                    continue
                # print(f'\t filtering by {f_attr}')
                for val in set(df[f_attr].values):
                    if val == 'None':
                        pass
                    f = Filter(f_attr, operation_str='==', value=val)
                    
                # df_f = f.do_operation(df)
                    # i_f = OutstandingInsight.create_insight_object(df, f, gb)
                    i_f = TrendInsight.create_insight_object(df, f, gb)
                    sc = SIG * i_f.score() + IMP * (f.do_operation(df).shape[0] / size) 
                    if str(f) not in top_insight_per_filter.keys():
                        top_insight_per_filter[str(f)] = (sc, i_f)
                    else:
                        if sc > top_insight_per_filter[str(f)][0]:
                            top_insight_per_filter[str(f)] = (sc, i_f)
                    if len(insights) < self.capacity:
                        heapq.heappush(insights, (sc, i_f))
                    else:
    # Equivalent to a push, then a pop, but faster
                        spilled_value = heapq.heappushpop(insights, (sc, i_f.insight_json()))
                    # if sc > max_score:
                    #     max_score = sc
                    #     attr_insight = i_f.insight_json()

            # insights.append(attr_insight)
            # print(i.insight_json())
        return top_insight_per_filter
    def mine_top_k(self, k=5, overlook_attrs = []):
        
        self._insights = []
        self._overlook_attrs = overlook_attrs
        self._top_insight_per_breakdown = {}
        # if self._
        if self._df.operation is not None and self._df.operation.type == 'groupby':
            return self.mine_top_k_grouped(k, overlook_attrs)
            
        prev_attrs = [f.attribute for f in self._df.get_retreival_query()]
        overlook_attrs = overlook_attrs + prev_attrs 
        # top_insight_per_filter['No'] = (0, None)
        for g_attr in self._group_attrs:
            g_attr_insights = []
            if g_attr in overlook_attrs:
                continue
            if  g_attr == 'Card_Category':
                continue
            print(f'grouping by {g_attr}')
            # attr_insight = None
            max_score = 0
            df = self._df#.copy()
            size = df.shape[0]
            dtype = df[g_attr].dtype.name
            n_bins = 5
            n_bins_f = 6
            g_attr_new = g_attr
            ser = df[g_attr]
            to_continue = self.to_continue(dtype, g_attr, ser)
            if 'binned' not in g_attr and ((dtype in ['int64', 'float64'] ) and len(ser.value_counts().values) > 10):
                continue
                # _, bins = pd.cut(ser, n_bins, retbins=True, duplicates='drop')
                # df[f'{g_attr}_binned'] = pd.cut(df[g_attr], bins=bins)
                # g_attr_new = f'{g_attr}_binned'
            #temporary!!!
            else:
                if (len(ser.value_counts().values)) > 10:
                    continue
            gb = GroupBy([g_attr_new], {g_attr_new: 'count'})
            # i = OutstandingInsight.create_insight_object(df, None, gb)
            if 'Income_Category' in g_attr:
                pass
            i = self.create_insight_object(df, None, gb)
            max_score = i.score()
            self._top_insight_per_breakdown[g_attr] = (max_score, i)
            # insights.append(i)
            heapq.heappushpop(self._insights, (max_score, i))
            # if(len(insights) == k+1):
                # pass
            for f_attr in df.columns:
                if f_attr in overlook_attrs:
                    continue
                if f_attr == 'Credit_Limit':
                    pass
                if 'binned' in f_attr:
                    continue
                if f_attr == g_attr:# and df[f_attr].dtype.name not in ['int64', 'float64']):
                    continue
                dtype = df[f_attr].dtype.name
                if len(df[f_attr].value_counts().values) > 10:
                    if not (dtype in ['int64', 'float64'] ) :
                        continue
                    _, bins = pd.cut(df[f_attr], n_bins_f, retbins=True, duplicates='drop')
                    df[f'{f_attr}_f_binned'] = pd.cut(df[f_attr], bins=bins)
                    f_attr = f'{f_attr}_f_binned'

                i = self.filter_mine(g_attr, f_attr, df, gb, k, size, [])
                if i is None:
                    continue
                # for i in insights:
                if len(g_attr_insights) < k:
                    heapq.heappush(g_attr_insights, (i[0], i[1]))
                else:
                    spilled_value = heapq.heappushpop(g_attr_insights, (i[0], i[1]))
        # ins = self._insights[1][1]
            if len(self._insights) < k:
                heapq.heappush(self._insights, g_attr_insights[-1])
            else:
                spilled_value = heapq.heappushpop(self._insights, (i[0], i[1]))
        # insight_view = ins.get_insight_view(df)
        # fig, ax = plt.subplots(layout='constrained')


        # idx = list(str(i) for i in insight_view.index)
        # vals = list(i[0] for i in insight_view.values)
        # ax.bar(idx, vals)
        # # ax = insight_view.plot.bar(rot=45, figsize=(7,7))
        # title = f"after {str(ins.filter)}"
        # if len(ins.target_retreival_query) > 0:
        #     rq = list(ins.target_retreival_query[0].dict().items())
        #     title = title + f',\nby \"{rq[0][1]} {rq[1][1]} {rq[2][1]}\"'
        # ax.set_title(title)

        # ax.set_xlabel(f"{ins.group_by_aggregate.group_attributes[0]}")
        # ax.set_ylabel(f"{list(ins.group_by_aggregate.agg_dict.items())[0]}")
        # plt.show()
        return self._insights
    

    def mine_top_k_grouped(self, k=5, overlook_attrs = []):
            
           
            # to_continue = self.to_continue(dtype, g_attr, ser)

        gb = self._df.operation
        df = self._df.prev_df
        size = df.shape[0]
        g_attr = gb.group_attributes[0]
        dtype = df[g_attr].dtype.name

        ser = df[g_attr]
        original_insight = self.create_insight_object(df, None, gb)
        max_score = original_insight.score()
        # self._top_insight_per_breakdown[g_attr] = (max_score, i)
            # insights.append(i)
        heapq.heappush(self._insights, (max_score, original_insight))
            # if(len(insights) == k+1):
                # pass
        # for f_attr in df.columns:
        #     if f_attr in overlook_attrs:
        #         continue
        #     if f_attr == 'Credit_Limit':
        #         pass
        #     if 'binned' in f_attr:
        #         continue
        #     if f_attr == g_attr:# and df[f_attr].dtype.name not in ['int64', 'float64']):
        #         continue

        #     i = self.filter_mine(g_attr, f_attr, df, gb, k, size, [])
        #         # for i in insights:
        #     if len(self._insights) < k:
        #         heapq.heappush(self._insights, (i[0], i[1]))
        #     else:
        #         spilled_value = heapq.heappushpop(self._insights, (i[0], i[1]))
        ins = sorted(self._insights, key= lambda x: -x[0])[0][1]
        # ins = self._insights[1][1]
        # insight_view = ins.get_insight_view(df)
        # original_insight_view = original_insight.get_insight_view(df)

        # ins = self._insights[1][1]
        insight_view = ins.get_insight_view(df)
        # fig, ax = plt.subplots()


        # idx = list(str(i) for i in insight_view.index)
        # vals = list(i[0] for i in insight_view.values)
        # ax.bar(idx, vals)
        # # ax = insight_view.plot.bar(rot=45, figsize=(7,7))
        # title = f"after {str(ins.filter)}"
        # if len(ins.target_retreival_query) > 0:
        #     rq = list(ins.target_retreival_query[0].dict().items())
        #     title = title + f',\nby \"{rq[0][1]} {rq[1][1]} {rq[2][1]}\"'
        # ax.set_title(title)

        # ax.set_xlabel(f"{ins.group_by_aggregate.group_attributes[0]}")
        # ax.set_ylabel(f"{list(ins.group_by_aggregate.agg_dict.items())[0]}")
        # plt.show()
        # fig, (ax, ax_original) = plt.subplots(2, layout='constrained')


        # idx = list(str(i) for i in insight_view.index)
        # vals1 = list(i[0] for i in insight_view.values)
        # vals2 = list(i[0] for i in original_insight_view.values)
        # ax.bar(idx, vals1)
        # ax_original.bar(idx, vals2)
        # # ax = insight_view.plot.bar(rot=45, figsize=(7,7))
        # title = f"after {str(ins.filter)}\n"
        # title_original = 'compared to the original insight:'
        # if len(ins.target_retreival_query) > 0:
        #     rq = list(ins.target_retreival_query[0].dict().items())
        #     title = title + f',\nby \"{rq[0][1]} {rq[1][1]} {rq[2][1]}\"'
        # ax.set_title(title)
        # ax_original.set_title(title_original)
        
        # ax.set_xlabel(f"{ins.group_by_aggregate.group_attributes[0]}")
        # # ax.set_xticklabels(ax.get_xticks(), rotation = 45)
        # ax.set_ylabel(f"{list(ins.group_by_aggregate.agg_dict.items())[0]}")
        # ax_original.set_xlabel(f"{ins.group_by_aggregate.group_attributes[0]}")
        # # ax_original.set_xticklabels(ax.get_xticks(), rotation = 45)

        # ax_original.set_ylabel(f"{list(ins.group_by_aggregate.agg_dict.items())[0]}")
        # plt.show()
        return self._insights
        pass
    
    def fix_val(self, val):
        try:
            return (val.left, val.right)
        except:
            return val
        
    def filter_mine(self, g_attr, f_attr, df, gb, k, size, filters):
        filter_insights = []
        f_attr_new = f'{f_attr}'
        operation_str = '=='
        vals = set(df[f_attr_new].values)
        # if df[f_attr_new].dtype.name in ['int64', 'float64']:
        #     _, bins = pd.cut(df[f_attr], 5, retbins=True, duplicates='drop')
        #     df[f'{f_attr}_f_binned'] = pd.cut(df[f_attr], bins=bins)
        #     f_attr_new = f'{f_attr}_f_binned'
        if 'binned' in f_attr:
            operation_str = 'between'
            f_attr_new = f'{f_attr[:-9]}'
            
            # vals = [(val.left, val.right) for val in set(df[f_attr_new].values)]
            # vals = [(float("{:.2f}".format(val.left)), float("{:.2f}".format(val.right))) for val in set(df[f_attr_new].values)]
        vals = set(df[f_attr].values)
        gb_all = GroupBy([g_attr, f_attr], gb.agg_dict)
        df_gb = gb_all.do_operation(df)
        for v in vals:
            f_df = df_gb.xs(v, level=f_attr)
            f = Filter(f_attr_new, operation_str, self.fix_val(v))
            i_f = self.create_insight_object(df, f, gb)
            sc =  i_f.score() #+ self.imp * (f_df.shape[0] / size) 
            if sc == 0:
                continue
            if len(filter_insights) < k:
                heapq.heappush(filter_insights, (sc, i_f))
            else:
                spilled_value = heapq.heappushpop(filter_insights, (sc, i_f))
       
       
        # for val in vals:
        #     if val == 'None':
        #         continue
        #     f = Filter(f_attr, operation_str=operation_str, value=val)
        #     filters_hat = filters + [f]
        #     # filters.append(f)
        #     mf = MultiFilter(filters_hat)
        #     f_df = mf.do_operation(df)
        #     # size = df.shape[0]
        #     i_f = self.create_insight_object(df, mf, gb)
        #     sc = self.sig * i_f.score() + self.imp * (f_df.shape[0] / size) 
        #     if sc == 1:
        #         pass
        #     if len(filter_insights) < k:
        #         heapq.heappush(filter_insights, (sc, i_f))
        #     else:
        #         spilled_value = heapq.heappushpop(filter_insights, (sc, i_f))
        
        # if len(filters_hat) < 2 and f_attr not in self.pairs:
        #     for f_attr_2 in f_df.columns:
        #         if f_attr_2 in self._overlook_attrs or f_attr_2 == f_attr:
        #             continue
        #         if 'binned' in f_attr_2:
        #             continue
        #         if f_attr_2 == g_attr:# and df[f_attr].dtype.name not in ['int64', 'float64']):
        #             continue
        #         i = self.filter_mine(g_attr, f_attr_2, df, gb, k,size, filters_hat)
        #         self.pairs.append(f_attr_2)
        #         # for i in insights:
        #         if len(filter_insights) < k:
        #             heapq.heappush(filter_insights, (i[0], i[1]))
        #         else:
        #             spilled_value = heapq.heappushpop(filter_insights, (i[0], i[1]))
        try:
            return filter_insights[-1]
        except:
            return None



