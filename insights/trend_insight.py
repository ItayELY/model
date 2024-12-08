import sys
sys.path.append("..") 

from insights.base_insight import BaseInsight
from operations.filter import Filter
from operations.group_by import GroupBy
from EDADataFrame import EDADataFrame
import numpy as np
from sklearn.metrics import r2_score

class   TrendInsight(BaseInsight):
    def __init__(self, df: EDADataFrame, filter: Filter | None, group_by_aggregate, highlight=None):
        super().__init__(df, filter, group_by_aggregate)
        self.highlight = highlight

        # self._insight_dict['type'] = 'attribution'
        self.type='trend'
        self._insight_dict['type'] = self.type
    
    @staticmethod
    def create_insight_object(df: EDADataFrame, filter: Filter, group_by_aggregate, highlight=None):
        insight = TrendInsight(df, filter, group_by_aggregate, highlight)
        return insight
    
    def internal_show_insight(self, df):
        explanation = 'When ' + self._df.get_path()
        if self.filter:
            explanation += f'filtering by {str(self.filter)} And '
        explanation += f'Selecting \"{list(self.group_by_aggregate.agg_dict.values())[0]}\" of \"{list(self.group_by_aggregate.agg_dict.keys())[0]}\" Grouped By \"{self.group_by_aggregate.group_attributes[0]}\"\n'
        explanation += f'There is an \"{self.highlight}\" trend (score {self._score}):\n'
        if self._score > 0.7:
            explanation += str(self.get_insight_view(self._df))
        print(explanation)
    
    def internal_score(self, df):
        if self._score: return self._score
        view = self.get_insight_view(df)
        # view = view / view.sum()
        
# plt.plot(x,y,"+", ms=10, mec="k")
        x = np.arange(view.shape[0])
        if len(x) < 3:
            self._score = 0
            return 0
        # l = len(x)
        y = np.array(view.values).flatten()
        z = np.polyfit(x, y, 1)
        y_hat = np.poly1d(z)(x)
        self._score = r2_score(y,y_hat) - 1/len(x)
        if z[0] > 0:
             self.highlight = "increasing"
        else:
             self.highlight = "decreasing"
        return self._score
# plt.plot(x, y_hat, "r--", lw=1)
        # text = f"$y={z[0]:0.3f}\;x{z[1]:+0.3f}$\n$R^2 = {r2_score(y,y_hat):0.3f}$"
# plt.gca().text(0.05, 0.95, text,transform=plt.gca().transAxes,
    #  fontsize=14, verticalalignment='top')

        
        # if not self.highlight:
        #     self.highlight = [k for (k, v) in zip(view.index, view.values) if v == max(view.values)][0] 
        # self._insight_dict['highlight'] = str(self.highlight)
        # mean_inc = view.mean()
        # mean_exc = ((view.mean() * view.count()) - view.loc[self.highlight]) / (view.count() - 1)
        # contribution_to_mean = (mean_inc) / (mean_inc + mean_exc) 
        # return contribution_to_mean[0]
    
    def __lt__(self, other):
        return self.score() < other.score()
         
