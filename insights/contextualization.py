import sys
sys.path.append("..") 

from EDADataFrame import EDADataFrame
from insights.base_insight import BaseInsight
from operations.filter import Filter
from operations.group_by import GroupBy
import operator

opposite_ops = {
    "==": "!=",
    ">": "<=",
    ">=": "<",
    "<": ">=",
    "<=": ">",
    "!=": "==",
    "between": lambda x, tup: x.apply(lambda item: tup[0] > item >= tup[1])
}
class Contextualize():
    def __init__(self, base_df, insight: BaseInsight, ref_insight: BaseInsight):
        self._base_df = base_df

        if base_df.operation.type != 'filter':
            base_df = base_df.prev_df
            ref_insight._df = ref_insight._df.prev_df
        self._base_size = base_df.shape[0]
        
        self._ref_df = ref_insight._df
        self._ref_size = self._ref_df.shape[0]

        self.prop = (self._ref_size /(self._ref_size + self._base_size))
        # if self._ref_size > self._base_size:
        #     self.prop = 1
        # else:
        #     self.prop = (self._ref_size / self._base_size)
        self._insight = insight
        self.score_original = self._insight.score()
        self.ref_insight = ref_insight
        self.score_ref = self.ref_insight.score()
        if not self.score_ref:
            self.score_ref = 0

        self._diff = False
        self.rel = min(self.score_original, self.score_ref) / max(self.score_original, self.score_ref)

        if self.ref_insight.highlight != self._insight.highlight:
            self._diff = True
            self.rel /= 2

        if not self.score_ref:
            self.rel = -1
    def distinction_score(self):

        self._score_dist = (1 - abs(self.rel)) * 0.8 + self.prop * 0.2 
        return self._score_dist
    
    def similar_score(self):
        self._score_sim = self.rel * 0.8 + self.prop * 0.2
        return self._score_sim
        