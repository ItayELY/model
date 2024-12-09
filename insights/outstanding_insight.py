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
        # Return cached score if it exists
            if self._score:
                return self._score
        
            size = df.shape[0]
            try:
            # Apply filter and get size of filtered data
                size_filtered = self.filter.do_operation(df).shape[0]
                pass
            except:
                size_filtered = size
        
        # Handle case where filtered data is empty
            if size_filtered == 0:
                self.size_filtered = 0.01
                return 0
            self.size_filtered = size_filtered
        
        # Generate insight view and calculate contribution metrics
            view = self.get_insight_view(df)
            if not self.highlight:
                self.highlight = [k for (k, v) in zip(view.index, view.values) if v[0] == max(view.values[0])][0]
            self._insight_dict['highlight'] = str(self.highlight)
        
        # Calculate components of the score
        # 1. Magnitude: Relative prominence of the highlight
            max_value = view.loc[self.highlight]
            mean_exc = ((view.mean() * view.count()) - max_value) / (view.count() - 1)
            magnitude = (max_value / (max_value + mean_exc))[0]
        
        # 2. Rarity: How uncommon is the highlight within the filtered data
            rarity = (1 - (view.loc[self.highlight] / view.sum()))[0]
        
        # 3. Impact: Logarithmic importance of the filter
            impact = math.log(size_filtered / size, 2)
            if impact > 10:
                return 0  # Excessively high impact is penalized
        
        # 4. Uniqueness: Boolean measure if the highlight is unique to the filtered data
            uniqueness = 1 #if self.is_highlight_unique(df, self.highlight) else 0.5  # Example uniqueness logic
        
        # Combine components into a final score
            weights = {'magnitude': 0.6, 'rarity': 0.2, 'impact': 0.2}
            final_score = (
                weights['magnitude'] * magnitude +
                weights['rarity'] * rarity +
                weights['impact'] * impact 
            )
        
        # Cache and return the score
            self._score = final_score
            if type(final_score) != type(1):
                pass
            return final_score
    
        except Exception as e:
            # print(f"Error calculating score: {e}")
            return 0

    
    def __lt__(self, other):
        return self.score() < other.score()
         
