import heapq
import sys
sys.path.append("..") 

from EDADataFrame import EDADataFrame
from operations.filter import Filter
from operations.filters_con import FiltersCon
from operations.filters_dis import FiltersDis

from operations.group_by import GroupBy
from insights.base_insight import BaseInsight

from insights.contextualization import Contextualize
from enumerate_filters import EnumFilters

import json
import pandas as pd
import warnings
warnings.filterwarnings("ignore")
SIG = 0.7
IMP = 1 - SIG

class RefMinerDivConqSample():
    def __init__(self, df: EDADataFrame, insight: BaseInsight) -> None:
        self._df = df
        self._insight = insight[1]


    def fix_val(self, val):
        try:
            return [val.left, val.right]
        except:
            return val
         
    def mine(self, overlook_attrs = [], k=3, level=5):
        top = self.exhaustive_miner_top_k(overlook_attrs, k, level)
                
        return top


    def exhaustive_miner(self, overlook_attrs, k, level):
        is_gb = False
        if self._df.operation is None:
            return []
        df = self._df
        if self._df.operation.type == 'groupby':
            is_gb = True
        df = self._df
        while df.operation is not None and level > 0:
            df = df.prev_df
            level -= 1
        size = df.shape[0]
        self.top_k_all = {'similar': [(0,None)], 'different': [(1,None)], 'optimized': [(0,None)]}
        self.top = {'similar': (0, None), 'different': (1, None), 'optimized': (0, None)}
        
        g_attr = self._insight.group_by_aggregate.group_attributes[0]
        if 'binned' in g_attr:
            _, bins = pd.cut(self._df[g_attr[:-7]], 5, retbins=True, duplicates='drop')
            df[f'{g_attr}'] = pd.cut(df[g_attr[:-7]], bins=bins)
            # g_attr_new = f'{g_attr}_binned'
        dtype = df[g_attr].dtype.name
        n_bins = 5
        n_bins_filter = 3
        gb = self._insight.group_by_aggregate


        ef = EnumFilters(df, overlook_attrs=overlook_attrs + [g_attr])
        filters, con, dis = ef.generate_filters(depth=1)
        
        n_filters = len(filters)
        return self.div_conq(filters, df, gb)
    




    def div_conq(self, filters, df, gb):
        cur_top = {'similar': (0, None), 'different': (1, None), 'optimized': (0, None)}
        n_filters = len(filters)
        if n_filters > 1:
            filters_1 = filters[:round(n_filters/2)]
            filters_2 = filters[round(n_filters/2):]
            next_top_1 = self.div_conq(filters_1, df, gb)
            next_top_2 = self.div_conq(filters_2, df, gb)

            if next_top_1['similar'][0] > next_top_2['similar'][0]:
                cur_top['similar'] = next_top_1['similar']
            else:
                cur_top['similar'] = next_top_2['similar']
            
            if next_top_1['different'][0] < next_top_2['different'][0]:
                cur_top['different'] = next_top_1['different']
            else:
                cur_top['different'] = next_top_2['different']

            if next_top_1['optimized'][0] > next_top_2['optimized'][0]:
                cur_top['optimized'] = next_top_1['optimized']
            else:
                cur_top['optimized'] = next_top_2['optimized']

            if next_top_1['similar'][1] is not None and next_top_2['similar'][1] is not None:
                merged_similar_f = FiltersDis([next_top_1['similar'][1].filter, next_top_2['similar'][1].filter])
                i_f = self._insight.create_insight_object(df, merged_similar_f, gb)
                sc = i_f.score()
                if sc == 0:
                    pass
                if self._insight.size_filtered / i_f.size_filtered > 10:
                    pass
                rel = min(self._insight._score, sc) / max(self._insight._score, sc)
                flag = True
                if i_f.highlight != self._insight.highlight:
                    rel /= 4
                    flag = False
                if rel > 0.8 and rel > next_top_1['similar'][0] and rel > next_top_2['similar'][0]:
                    cur_top['similar'] = (rel, i_f)

            if next_top_1['different'][1] is not None and next_top_2['different'][1] is not None:
                merged_different_f = FiltersDis([next_top_1['different'][1].filter, next_top_2['different'][1].filter])
                i_f = self._insight.create_insight_object(df, merged_different_f, gb)
                sc = i_f.score()
                if sc == 0:
                    pass
                if self._insight.size_filtered / i_f.size_filtered > 10:
                    pass
                rel = min(self._insight._score, sc) / max(self._insight._score, sc)
                flag = True
                if i_f.highlight != self._insight.highlight:
                    rel /= 4
                    flag = False
                elif rel < 0.8 and rel < next_top_1['different'][0] and rel < next_top_1['different'][0]:
                    rel += (self._insight.size_filtered / i_f.size_filtered) / 10
                    if rel < next_top_1['different'][0] and rel < next_top_2['different'][0]:
                        cur_top['different'] = (rel, i_f)


            if next_top_1['optimized'][1] is not None and next_top_2['optimized'][1] is not None:
                merged_opt_f = FiltersDis([next_top_1['optimized'][1].filter, next_top_2['optimized'][1].filter])
                i_f = self._insight.create_insight_object(df, merged_opt_f, gb)
                sc = i_f.score()
                if sc == 0:
                    pass
                if self._insight.size_filtered / i_f.size_filtered > 10:
                    pass
                rel = min(self._insight._score, sc) / max(self._insight._score, sc)
                flag = True
                if i_f.highlight != self._insight.highlight:
                    rel /= 4
                    flag = False
                if self._insight._score / sc < 1 and self._insight._score / sc  > next_top_1['optimized'][0] and self._insight._score / sc  > next_top_1['optimized'][0] and flag:
                    rel += (i_f.size_filtered / self._insight.size_filtered) / 10
                    if rel > next_top_1['optimized'][0] and rel > next_top_2['optimized'][0]:
                        cur_top['optimized'] = (rel, i_f)
            return cur_top





        
        for f in filters:
            i_f = self._insight.create_insight_object(df, f, gb)
            sc = i_f.score()
            if sc == 0:
                continue
            if self._insight.size_filtered / i_f.size_filtered > 10:
                continue
            rel = min(self._insight._score, sc) / max(self._insight._score, sc)
            flag = True
            if i_f.highlight != self._insight.highlight:
                rel /= 4
                flag = False
            if rel > 0.8 and rel > cur_top['similar'][0]:
                cur_top['similar'] = (rel, i_f)
                        # heapq.heappush(insights, (sc, i_f))
            elif rel < 0.8 and rel < cur_top['different'][0]:
                rel += (self._insight.size_filtered / i_f.size_filtered) / 10
                if rel < cur_top['different'][0]:
                    cur_top['different'] = (rel, i_f)
            if self._insight._score / sc < 1 and self._insight._score / sc  > cur_top['optimized'][0] and flag:
                rel += (i_f.size_filtered / self._insight.size_filtered) / 10
                if rel > cur_top['optimized'][0]:
                    cur_top['optimized'] = (rel, i_f)
        return cur_top




    def exhaustive_miner_top_k(self, overlook_attrs, k, level):
        is_gb = False
        if self._df.operation is None:
            return []
        df = self._df
        if self._df.operation.type == 'groupby':
            is_gb = True
        df = self._df
        while df.operation is not None and level > 0:
            df = df.prev_df
            level -= 1
        
        self.samp_df = EDADataFrame(df.sample(frac=0.1, replace=True, random_state=1))
        size = df.shape[0]
        self.top_k_all = {'similar': [(0,None)], 'different': [(1,None)], 'optimized': [(0,None)]}
        
        g_attr = self._insight.group_by_aggregate.group_attributes[0]
        if 'binned' in g_attr:
            _, bins = pd.cut(self._df[g_attr[:-7]], 5, retbins=True, duplicates='drop')
            df[f'{g_attr}'] = pd.cut(df[g_attr[:-7]], bins=bins)
            self.samp_df[f'{g_attr}'] = pd.cut(self.samp_df[g_attr[:-7]], bins=bins)
            # g_attr_new = f'{g_attr}_binned'
        gb = self._insight.group_by_aggregate


        ef = EnumFilters(df, overlook_attrs=overlook_attrs + [g_attr])
        filters, con, dis = ef.generate_filters(depth=1)
        
        return self.div_conq_top_k(filters, df, gb, k)
        
    def div_conq_top_k(self, filters, df, gb, k):
        cur_top = {'similar': [], 'different': [], 'optimized': []}
        n_filters = len(filters)
        if n_filters > 1:
            filters_1 = filters[:round(n_filters/2)]
            filters_2 = filters[round(n_filters/2):]
            next_top_1 = self.div_conq_top_k(filters_1, df, gb, k)
            next_top_2 = self.div_conq_top_k(filters_2, df, gb, k)

            next_top_1['different'] = list(next_top_1['different'])
            for tup in next_top_2['different']:
                if len(next_top_1['different']) < k:
                    heapq.heappush(next_top_1['different'], tup)
            
                else: 
                    heapq.heappushpop(next_top_1['different'], tup)

            for tup in next_top_2['similar']:
                if len(next_top_1['similar']) < k:
                    heapq.heappush(next_top_1['similar'], tup)
            
                else: 
                    heapq.heappushpop(next_top_1['similar'], tup)

           
            return next_top_1





        
        for f in filters:
            i_f = self._insight.create_insight_object(self.samp_df, f, gb)
            ctx = Contextualize(self.samp_df, self._insight, i_f)
            if len(cur_top['different']) < k:
                heapq.heappush(cur_top['different'], (ctx.distinction_score(), i_f))
            
            else: 
                heapq.heappushpop(cur_top['different'], (ctx.distinction_score(), i_f))

            if len(cur_top['similar']) < k:
                heapq.heappush(cur_top['similar'], (ctx.similar_score(), i_f))
            
            else: 
                heapq.heappushpop(cur_top['similar'], (ctx.similar_score(), i_f))
            
        return cur_top