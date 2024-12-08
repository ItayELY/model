import sys
sys.path.append("..") 
TH = 0.3
from operations.filter import Filter
from EDADataFrame import EDADataFrame
from operations.group_by import GroupBy

class BaseInsight():
    def __init__(self, df: EDADataFrame, filter: Filter | None, group_by_aggregate : GroupBy | None):
        self.filter = filter
        self.group_by_aggregate = group_by_aggregate
        self._score = None
        self._df = df
        self.target_retreival_query = df.get_retreival_query()
        self.target_retreival_query_readable = df.get_retreival_query_readable()
        self._insight_dict = {
            'insight_query': 
                {
                    'target_retreival_query': self.target_retreival_query_readable,
                    'filter': self.filter.dict() if self.filter is not None else None, 
                    'group_by_aggregate': self.group_by_aggregate.dict()}
                }
        self.highlight = None
    def get_insight_view(self, df=None):
        if df is None:
            df = self._df
        df_insight_view = df.copy()
        if self.filter:
            df_insight_view = self.filter.do_operation(df_insight_view)
        df_insight_view = self.group_by_aggregate.do_operation(df_insight_view)
        return df_insight_view
    def show_insight(self, df=None):
        if df is None:
            df = self._df
        if not self._score:
            self.score(df)
        if self._score < TH:
            print(f"no interesting {self.type} insight here (score is {0})")
            return
        self.internal_show_insight(df)
    
    def score(self, df=None):
        if df is None:
            df = self._df
        self._score = self.internal_score(df)
        self._insight_dict['score'] = self._score
        # self._insight_dict['score'] = self._score
        return self._score
    
    @staticmethod
    def create_insight_object(df: EDADataFrame, filter: Filter, group_by_aggregate, target=None):
        return None
    def insight_json(self):
        return self._insight_dict
        