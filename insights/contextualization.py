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
        self._base_size = base_df.shape[0]

        self._insight = insight
        self.score_original = self._insight.score()
        self.ref_insight = ref_insight
        self.score_ref = self.ref_insight.score()

        self._diff = False
        self.rel = min(self.score_original, self.score_ref) / max(self.score_original, self.score_ref)

        if self.ref_insight.highlight != self._insight.highlight:
            self._diff = True
            self.rel /= 2


    def distinction_score(self):
        self._score_dist = 1 - self.rel
        return self._score_dist
    
    def similar_score(self):
        self._score_sim = self.rel
        return self._score_sim
        