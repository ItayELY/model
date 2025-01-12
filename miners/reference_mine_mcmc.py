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
import random
import numpy as np
import warnings
warnings.filterwarnings("ignore")
SIG = 0.7
IMP = 1 - SIG

class RefMinerMCMC():
    def __init__(self, df: EDADataFrame, insight: BaseInsight) -> None:
        self._df = df
        self._insight = insight[1]

    def mine(self, overlook_attrs = [], k=3, level=5):
        top = self.exhaustive_miner(overlook_attrs, k, level)
                
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
        n_bins = 5

        g_attr = self._insight.group_by_aggregate.group_attributes[0]

        if 'binned' in g_attr:
            _, bins = pd.cut(self._df[g_attr[:-7]], n_bins, retbins=True, duplicates='drop')
            df[f'{g_attr}'] = pd.cut(df[g_attr[:-7]], bins=bins)
            # g_attr_new = f'{g_attr}_binned'
        dtype = df[g_attr].dtype.name
        n_bins_filter = 3
        gb = self._insight.group_by_aggregate


        ef = EnumFilters(df, overlook_attrs=overlook_attrs + [g_attr, g_attr[:-7]], n_bins_f=3)
        filters, con, dis = ef.generate_filters(depth=1)
        
        n_filters = len(filters)
        result_similar = self.mcmc_filter_selection_similar(df, filters, gb, iterations=500, initial_temp=10)    
        result_diff = self.mcmc_filter_selection_different(df, filters, gb, iterations=200, initial_temp=10)
        print(self.log)
        return {'similar': [result_similar], 'different': [result_diff]}
        # return self.top
        # return self.div_conq(filters, df, gb)
    

    def mcmc_filter_selection_similar(self, df, filters, gb, iterations=1000, initial_temp=1.0):
        self.log = {}
        current_state = []#random.sample(filters, 1)
        # current_predicate = " and ".join(current_state)
        # current_score = evaluate_predicate(dataframe, current_predicate)
        
        f_con = FiltersCon(current_state)
        i_f = self._insight.create_insight_object(df, f_con, gb)
        ctx = Contextualize(df, self._insight, i_f)
        current_score = ctx.similar_score()
        best_state = current_state[:]
        best_ins = (current_score, i_f)

        best_score = current_score
        relevants = filter(lambda x: x.attribute == 'Credit_Limit', filters)
        relevants = list(relevants)
        for step in range(iterations):

        # Propose a new state
            new_state = current_state[:]
            if random.random() < 0.6 and len(new_state) > 0:  # Remove a filter
                new_state.remove(random.choice(new_state))
            else:  # Add or replace a filter
                if random.random() < 0.5:
                    new_filter = random.choice(relevants)
                else:
                    new_filter = random.choice(filters)
                if random.random() < 0.3 and new_filter not in new_state:
                    pass
                    new_state.append(new_filter)
                    
                else:
                    # new_state.append(new_filter)
                    try:
                        new_state.remove(random.choice(new_state))
                    except:
                        pass
                    # new_state = [new_filter]
                    new_state.append(new_filter)
                    pass
            # new_predicate = " and ".join(new_state)
            f_con_new = FiltersCon(new_state)
            i_f_new = self._insight.create_insight_object(df, f_con_new, gb)
            ctx_new = Contextualize(df, self._insight, i_f_new)
            new_score = ctx_new.similar_score() 

        # Acceptance criteria
            if new_score > current_score:# or random.random() < np.exp((current_score - new_score) / temp):
                current_state = new_state
                current_score = new_score

        # Update best state
            if current_score > best_score:
                best_state = current_state
                best_ins = (new_score, i_f_new)
                best_score = current_score
            self.log[step] = best_score

        return best_ins
    



    def mcmc_filter_selection_different(self, df, filters, gb, iterations=1000, initial_temp=1.0):
        self.log_different = {}
        current_state = []#random.sample(filters, 1)
        # current_predicate = " and ".join(current_state)
        # current_score = evaluate_predicate(dataframe, current_predicate)
        f_con = FiltersCon(current_state)
        i_f = self._insight.create_insight_object(df, f_con, gb)
        ctx = Contextualize(df, self._insight, i_f)
        current_score = ctx.distinction_score()
        best_state = current_state[:]
        best_ins = (current_score, i_f)

        best_score = current_score
    
        for step in range(iterations):
            temp = initial_temp / (step + 1)  # Decrease temperature over time

        # Propose a new state
            new_state = current_state[:]
            if random.random() < 0.2 and len(new_state) > 1:  # Remove a filter
                new_state.remove(random.choice(new_state))
            else:  # Add or replace a filter
                new_filter = random.choice(filters)
                if random.random() < 0.2 and new_filter not in new_state:
                    new_state.append(new_filter)
                else:
                    try:
                        new_state.remove(random.choice(new_state))
                    except:
                        pass
                    new_state.append(new_filter)
                    # new_state = [new_filter]

            # new_predicate = " and ".join(new_state)
            f_con_new = FiltersCon(new_state)
            i_f_new = self._insight.create_insight_object(df, f_con_new, gb)
            ctx_new = Contextualize(df, self._insight, i_f_new)
            new_score = ctx_new.distinction_score()

        # Acceptance criteria
            if new_score > current_score:# or random.random() < np.exp((current_score - new_score) / temp):
                current_state = new_state
                current_score = new_score

        # Update best state
            if current_score > best_score:
                best_state = current_state
                best_ins = (new_score, i_f_new)
                best_score = current_score
            self.log_different[step] = best_score


        return best_ins

