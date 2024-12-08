import sys
sys.path.append("..") 

from insights.base_insight import BaseInsight
from operations.filter import Filter
from operations.group_by import GroupBy
from EDADataFrame import EDADataFrame

class AttributionInsight(BaseInsight):
    def __init__(self, df: EDADataFrame, filter: Filter | None, group_by_aggregate, highlight=None):
        super().__init__(df, filter, group_by_aggregate)
        self.highlight = highlight

        # self._insight_dict['type'] = 'attribution'
        self.type='attribution'
    
    @staticmethod
    def create_insight_object(df: EDADataFrame, filter: Filter, group_by_aggregate, highlight=None):
        insight = AttributionInsight(df, filter, group_by_aggregate, highlight)
        return insight
    
    def internal_show_insight(self, df):
        explanation = 'When ' + self._df.get_path()
        if self.filter:
            explanation += f'filtering by {str(self.filter)} And '
        explanation += f'Selecting \"{list(self.group_by_aggregate.agg_dict.values())[0]}\" of \"{list(self.group_by_aggregate.agg_dict.keys())[0]}\" Grouped By \"{self.group_by_aggregate.group_attributes[0]}\"\n'
        explanation += f'The \"{self.highlight}\" group is the most significant:\n'
        explanation += str(self.get_insight_view(self._df))
        print(explanation)
    
    def internal_score(self, df):
        view = self.get_insight_view(df)
        view = view / view.sum()
        self.highlight = [k for (k, v) in zip(view.index, view.values) if v == max(view.values)][0] 
        if max(view.values) >= 0.5:
            self._insight_dict['highlight'] = self.highlight
            return 1
        else: return 0 
