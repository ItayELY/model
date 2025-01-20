import operator
import sys
from operations.filter import Filter
from insights.outstanding_insight import OutstandingInsight
from final_contextualize.base_contextualize import BaseContextualize
from insights.contextualization import Contextualize


import pandas as pd
sys.path.append("..") 
from EDADataFrame import EDADataFrame
from operations.operation import Operation

class GlobalContextualize(BaseContextualize):
    def __init__(self, df, insight):
        super().__init__(df, insight)
        self.levels_neighbors = {}
        self.n_bins = 200
        pass
    
    def get_neighbors(self, level=1, given_op=None, given_v=None):
        orig_level = level
        neighbors = []
        src_df = self.df
        left_operations = []
        operation = self.insight.filter
        while level != 0:
            if src_df.operation is None:
                return []
            left_operations.append(src_df.operation)
            if src_df.operation.type != 'filter':
                src_df = src_df.prev_df

                continue
            operation = src_df.operation
            src_df = src_df.prev_df
            level -= 1
        left_operations.reverse()
        is_filter = (operation.type == 'filter')
        if not is_filter:
            return []
        
        attribute = operation.attribute
        value = operation.value
        oper = '=='
        # print(f'original filter: {str(operation)}')
        if given_v is not None:
            adj_f = Filter(attribute, given_op, given_v)
            neigh_df = adj_f.do_operation(src_df)
            if len(left_operations) > 1:
                    for op in left_operations[1:]:
                        neigh_df = op.do_operation(neigh_df)
            return neigh_df, adj_f

        dtype = src_df[attribute].dtype.name
        n_bins = self.n_bins
        attribute_new = attribute
        if dtype in ['int64', 'float64'] and len(set(src_df[f'{attribute}'])) > 10: 
            _, bins = pd.cut(src_df[attribute], n_bins, retbins=True, duplicates='drop')
            src_df[f'{attribute}_binned'] = pd.cut(src_df[attribute], bins=bins)
            src_df[f'{attribute}_binned'] = src_df.apply(lambda x: (x[f'{attribute}_binned'].left, x[f'{attribute}_binned'].right), axis = 1)
            attribute_new = f'{attribute}_binned'
            oper = 'between'

        for v in set(src_df[attribute_new].values):
            if v == value:
                continue
            else:
                adj_f = Filter(attribute, oper, v)
                neigh_df = adj_f.do_operation(src_df)
                if len(left_operations) > 1:
                    for op in left_operations[1:]:
                        neigh_df = op.do_operation(neigh_df)
                neighbors.append((adj_f, neigh_df))
                # print(f'\treference df: {str(neigh_df.get_path())}') 
        self.levels_neighbors[orig_level] = neighbors
    
    def concat(self, lst):
        if not lst:
            return lst
        if type(lst[0]) == type((1, 2)):
            lst.sort(key = lambda x: x[0])
            ranges = []
            cur = lst[0][0]
            for i in range(0, len(lst)-1):
                if lst[i+1][0] == lst[i][1]:
                    continue
                else:
                    ranges.append((cur, lst[i][1]))
                    cur = lst[i+1][0]
            ranges.append((cur, lst[-1][1]))
        else: return lst
        return ranges

    def contextualize(self, level=1):
        if not level in self.levels_neighbors.keys():
            self.get_neighbors(level)
        similar = []
        similar_span = []
        dist = []
        dist_span = []

        similar_span_to_return = []
        dist_span_to_return = []
        for (f, df) in self.levels_neighbors[level]:
            in2 = OutstandingInsight.create_insight_object(df, self.insight.filter, self.insight.group_by_aggregate)
            ctx_scoring = Contextualize(self.df, self.insight, in2)
            score_sim = ctx_scoring.similar_score()
            score_dist = ctx_scoring.distinction_score()
            
            if score_sim > score_dist:
                similar.append((score_sim, df))
                similar_span.append(f.value)
                similar_span_to_return.append((score_sim, f.value))
            else:
                dist.append((score_dist, df))
                dist_span.append(f.value)
                dist_span_to_return.append((score_dist, f.value))
        self.sim_ranges = self.concat(similar_span)
        self.dist_ranges = self.concat(dist_span)

        self.sim_insights = []
        self.dist_insights = []
        for s in self.sim_ranges:
            try:
                df, f = self.get_neighbors(level, 'between', s)
            except:
                df, f = self.get_neighbors(level, '==', s)
            in2 = OutstandingInsight.create_insight_object(df, self.insight.filter, self.insight.group_by_aggregate)
            ctx_scoring = Contextualize(self.df, self.insight, in2)
            score_sim = ctx_scoring.similar_score()
            self.sim_insights.append((in2, f, df.shape[0]))
            # print(f'similar insight (score {score_sim}): {in2.show_insight()}')
        
        for s in self.dist_ranges:
            try:
                df, f = self.get_neighbors(level, 'between', s)
            except:
                df, f = self.get_neighbors(level, '==', s)
            in2 = OutstandingInsight.create_insight_object(df, self.insight.filter, self.insight.group_by_aggregate)
            ctx_scoring = Contextualize(self.df, self.insight, in2)
            score_dist = ctx_scoring.distinction_score()
            self.dist_insights.append((in2, f, df.shape[0]))
            # print(f'distinct insight (score {score_dist}): {in2.show_insight()}')
        self.sim_insights.sort(key=lambda x: -x[2])
        self.dist_insights.sort(key=lambda x: -x[2])


    
        self.sim_insights_local = []
        self.dist_insights_local = []
        for s in similar_span:
            try:
                df, f = self.get_neighbors(level, 'between', s)
            except:
                df, f = self.get_neighbors(level, '==', s)
            in2 = OutstandingInsight.create_insight_object(df, self.insight.filter, self.insight.group_by_aggregate)
            ctx_scoring = Contextualize(self.df, self.insight, in2)
            score_sim = ctx_scoring.similar_score()
            self.sim_insights_local.append((in2, score_sim, f, df.shape[0]))
            # print(f'similar insight (score {score_sim}): {in2.show_insight()}')
        
        for s in dist_span:
            try:
                df, f = self.get_neighbors(level, 'between', s)
            except:
                df, f = self.get_neighbors(level, '==', s)
            in2 = OutstandingInsight.create_insight_object(df, self.insight.filter, self.insight.group_by_aggregate)
            ctx_scoring = Contextualize(self.df, self.insight, in2)
            score_dist = ctx_scoring.distinction_score()
            self.dist_insights_local.append((in2, score_dist, f, df.shape[0]))
            # print(f'distinct insight (score {score_dist}): {in2.show_insight()}')
        self.sim_insights_local.sort(key=lambda x: -x[1])
        self.dist_insights_local.sort(key=lambda x: -x[1])
        return similar_span_to_return, dist_span_to_return, f.attribute
        # print(f'similar: {similar}')
        # print(f'dist: {dist}')
        
