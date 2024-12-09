import heapq
import sys
sys.path.append("..") 

from EDADataFrame import EDADataFrame
from operations.filter import Filter

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

class RefMiner():
    def __init__(self, df: EDADataFrame, insight: BaseInsight) -> None:
        self._df = df
        self._insight = insight[1]


    def fix_val(self, val):
        try:
            return [val.left, val.right]
        except:
            return val
         
    def mine(self, overlook_attrs = [], k=3, level=1):
        top = self.exhaustive_miner(overlook_attrs, k, level)
                
        return top


    def cont_attr(self):
        pass

    def get_span(self, df, f_attr, f_attr_for_insight, operation_str = 'between'):
        gb = self._insight.group_by_aggregate
        total_val = None
        prevs = {'similar': 0, 'different': 1, 'optimized': 0}
        i = 0
        df['sortkey']=df[f_attr].map(lambda x : x.left)
        sorted_vals = sorted(set(df[f_attr].values), key=lambda x : x.left)
        for val in sorted_vals:
            i += 1
            val = self.fix_val(val)
            total_val = val.copy()
            if (self._insight.filter is not None) and (str(val) == str(self._insight.filter.value)):
                continue
            if total_val is not None:
                total_val[1] = val[1]
            else:
                total_val = val
            f = Filter(f_attr_for_insight, operation_str=operation_str, value=total_val.copy())
                    
            i_f = self._insight.create_insight_object(df, f, gb)
            if f_attr == 'speechiness_binned':
                pass
            sc = i_f.score()
            if sc == 0:
                continue
            rel = min(self._insight._score, sc) / max(self._insight._score, sc)
            if self._insight.size_filtered / i_f.size_filtered > 10:
                continue
            flag = True
            if i_f.highlight != self._insight.highlight:
                rel /= 4
                flag = False
            score_similar = rel
            score_different = rel
            score_opt = self._insight._score / sc
            if score_opt > 1:
                score_opt = 0

            
            if score_similar > 0.8 and score_similar > self.top['similar'][0]:
                self.top['similar'] = (score_similar, i_f)
                        # heapq.heappush(insights, (sc, i_f))
            elif score_different < 0.6 and score_different < self.top['different'][0]:
                self.top['different'] = (score_different, i_f)
            if score_opt  > self.top['optimized'][0] and flag:
                self.top['optimized'] = (rel, i_f)
         
                 
            for val2 in sorted_vals[i:]:
                t = type(val2)
                val2 = self.fix_val(val2)
                if (self._insight.filter is not None) and (str(val2) == str(self._insight.filter.value)):
                    continue
            # if total_val is not None:
                total_val[1] = val2[1]
            # else:
                # total_val = val
                f1 = Filter(f_attr_for_insight, operation_str=operation_str, value=total_val)
                    
                i_f_1 = self._insight.create_insight_object(df, f1, gb)
                sc = i_f_1.score()
                if sc == 0:
                    continue
                rel = min(self._insight._score, sc) / max(self._insight._score, sc)
                flag = True
                if i_f_1.highlight != self._insight.highlight:
                    rel /= 4
                    flag = False
                score_similar = rel
                score_different = rel
                score_opt = self._insight._score / sc
                if score_opt > 1:
                    score_opt = 0

            
                if score_similar > 0.8 and score_similar > self.top['similar'][0]:
                    self.top['similar'] = (score_similar, i_f_1)
                        # heapq.heappush(insights, (sc, i_f))
                elif score_different < 0.6 and score_different < self.top['different'][0]:
                    self.top['different'] = (score_different, i_f_1)
                if score_opt  > self.top['optimized'][0] and flag:
                    self.top['optimized'] = (rel, i_f_1)

    def basic_miner(self, overlook_attrs, k, level):
        is_gb = False
        if self._df.operation is None:
            return []
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
        for f_attr in df.columns:
            cont = False
            f_attr_for_insight = f_attr
            operation_str='=='
            if f_attr in gb.group_attributes or f'{f_attr}_binned' in gb.group_attributes or f_attr in overlook_attrs or f_attr[:-7] in overlook_attrs:
                continue
            if  'f_binned' in f_attr:
                continue
            if 'binned' in f_attr:
                
                operation_str='between'
                _, bins = pd.cut(df[f_attr[:-7]], 5, retbins=True, duplicates='drop')
                df[f'{f_attr}'] = pd.cut(df[f_attr[:-7]], bins=bins)
                f_attr_for_insight = f_attr[:-7]
            dtype = df[f_attr].dtype.name
            if (dtype in ['int64', 'float64'] ) and len(df[f_attr].value_counts().values) > 15:
                # if is_gb:
                    operation_str='between'
                    cont = True
                    _, bins = pd.cut(df[f_attr], 5, retbins=True, duplicates='drop')
                    # f_attr = f_attr + 
                    df[f'{f_attr}_binned'] = pd.cut(df[f_attr], bins=bins)
                    f_attr = f'{f_attr}_binned'
                    f_attr_for_insight = f_attr[:-7]
                # else:
                    # continue
            #temporary!!!
            else:
                if len(df[f_attr].value_counts().values) > 10:
                    continue
            if cont:
                self.get_span(df, f_attr, f_attr_for_insight)
                continue
            for val in set(df[f_attr].values):
                    t = type(val)
                    val = self.fix_val(val)
                    if (self._insight.filter is not None) and (str(val) == str(self._insight.filter.value)):
                        continue
                    f = Filter(f_attr_for_insight, operation_str=operation_str, value=val)
                    
                    i_f = self._insight.create_insight_object(df, f, gb)
                    sc = i_f.score()
                    if sc == 0:
                        continue
                    rel = min(self._insight._score, sc) / max(self._insight._score, sc)
                    flag = True
                    if i_f.highlight != self._insight.highlight:
                         rel /= 4
                         flag = False
                    if rel > 0.8 and rel > self.top['similar'][0]:
                        self.top['similar'] = (rel, i_f)
                        # heapq.heappush(insights, (sc, i_f))
                    elif rel < 0.8 and rel < self.top['different'][0]:
                        self.top['different'] = (rel, i_f)
                    if self._insight._score / sc < 1 and self._insight._score / sc  > self.top['optimized'][0] and flag:
                        self.top['optimized'] = (rel, i_f)
                
        return self.top
    


    def exhaustive_miner(self, overlook_attrs, k, level):
        is_gb = False
        if self._df.operation is None:
            return []
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
        filters, con, dis = ef.generate_filters(depth=2)
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
            if rel > 0.8 and rel > self.top['similar'][0]:
                self.top['similar'] = (rel, i_f)
                        # heapq.heappush(insights, (sc, i_f))
            elif rel < 0.8 and rel < self.top['different'][0]:
                rel += (self._insight.size_filtered / i_f.size_filtered) / 10
                if rel < self.top['different'][0]:
                    self.top['different'] = (rel, i_f)
            if self._insight._score / sc < 1 and self._insight._score / sc  > self.top['optimized'][0] and flag:
                rel += (i_f.size_filtered / self._insight.size_filtered) / 10
                if rel > self.top['optimized'][0]:
                    self.top['optimized'] = (rel, i_f)
                # self.top['optimized'] = (rel, i_f)

        for f in con:
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
            if rel > 0.8 and rel > self.top['similar'][0]:
                self.top['similar'] = (rel, i_f)
                        # heapq.heappush(insights, (sc, i_f))
            elif rel < 0.8 and rel < self.top['different'][0]:
                rel += (self._insight.size_filtered / i_f.size_filtered) / 10
                if rel < self.top['different'][0]:
                    self.top['different'] = (rel, i_f)
            if self._insight._score / sc < 1 and self._insight._score / sc  > self.top['optimized'][0] and flag:
                rel += (i_f.size_filtered / self._insight.size_filtered) / 10
                if rel > self.top['optimized'][0]:
                    self.top['optimized'] = (rel, i_f)


        for f in dis:
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
            if rel > 0.8 and rel > self.top['similar'][0]:
                self.top['similar'] = (rel, i_f)
                        # heapq.heappush(insights, (sc, i_f))
            elif rel < 0.8 and rel < self.top['different'][0]:
                rel += (self._insight.size_filtered / i_f.size_filtered) / 10
                if rel < self.top['different'][0]:
                    self.top['different'] = (rel, i_f)
            if self._insight._score / sc < 1.2 and self._insight._score / sc  > self.top['optimized'][0] and flag:
                rel += (i_f.size_filtered / self._insight.size_filtered) / 10
                if rel > self.top['optimized'][0]:
                    self.top['optimized'] = (rel, i_f) 
                
        return self.top
    
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
       