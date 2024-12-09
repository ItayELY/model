import sys
sys.path.append("..") 
import math

from insights.base_insight import BaseInsight
from operations.filter import Filter
from operations.group_by import GroupBy
from EDADataFrame import EDADataFrame

class OutstandingInsight(BaseInsight):
    def __init__(self, df: EDADataFrame, filter, group_by_aggregate, highlight=None):
        super().__init__(df, filter, group_by_aggregate)
        self.highlight = highlight

        self.sig = 0.7
        self.imp = 1-self.sig
        # self._insight_dict['type'] = 'attribution'
        self.type='outstanding'
        self._insight_dict['type'] = self.type
    
    @staticmethod
    def create_insight_object(df: EDADataFrame, filter: Filter, group_by_aggregate, highlight=None):
        insight = OutstandingInsight(df, filter, group_by_aggregate, highlight)
        return insight
    
    def internal_show_insight(self, df):
        explanation = 'When ' + self._df.get_path()
        if self.filter:
            explanation += f'filtering by {str(self.filter)} And '
        explanation += f'Selecting \"{list(self.group_by_aggregate.agg_dict.values())[0]}\" of \"{list(self.group_by_aggregate.agg_dict.keys())[0]}\" Grouped By \"{self.group_by_aggregate.group_attributes[0]}\"\n'
        explanation += f'The \"{self.highlight}\" group is the most significant (score {self._score}):\n'
        if self._score > 0.7:
            explanation += str(self.get_insight_view(self._df))
        print(explanation)
    
    def internal_score(self, df):
        try:
            if self._score: return self._score
            size = df.shape[0]
            try:
                size_filtered = self.filter.do_operation(df).shape[0]
            except:
                size_filtered = size
                    # Handle case where filtered data is empty
            if size_filtered == 0:
                self.size_filtered = 0.01
                return 0
            self.size_filtered = size_filtered 
            view = self.get_insight_view(df)
        # view = view / view.sum()
            if not self.highlight:
                self.highlight = [k for (k, v) in zip(view.index, view.values) if v == max(view.values)][0] 
            self._insight_dict['highlight'] = str(self.highlight)
            mean_inc = view.mean()
            mean_exc = ((view.mean() * view.count()) - view.loc[self.highlight]) / (view.count() - 1)
            contribution_to_mean = (mean_inc) / (mean_inc + mean_exc) 
            imp = math.log((size / size_filtered), 2)
            if imp > 10:
                return 0
            sc = self.sig * contribution_to_mean[0] * imp# + self.imp * (size_filtered/size)
            if sc is None:
                pass
            return self.sig * contribution_to_mean[0] + self.imp * (size_filtered/size)
        except Exception as e:
            
            pass
            return 0
    
    def __lt__(self, other):
        return self.score() < other.score()
         
