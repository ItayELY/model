import sys
sys.path.append("..") 

from EDADataFrame import EDADataFrame
from operations.filter import Filter
from operations.group_by import GroupBy
from insights.attribution_insight import AttributionInsight
from insights.contextualization import Contextualize
from insights.trend_insight import TrendInsight

from miners.insight_miner import Miner
import json
import pandas as pd
import warnings
warnings.filterwarnings("ignore")

class TrendMiner(Miner):
    def __init__(self, df: EDADataFrame, group_attrs: list | None, agg_attrs: list | None) -> None:
        super().__init__(df, group_attrs, agg_attrs)
        self.sig = 0.9
        self.imp = 0.1

    def inner_mine(self):
        attrs = self._df.columns
        insights = []
        for g_attr in self._group_attrs:
            df = self._df.copy()

            
            dtype = df[g_attr].dtype.name
            n_bins = 10
            if dtype in ['int64', 'float64']:
                _, bins = pd.cut(df[g_attr], n_bins, retbins=True, duplicates='drop')
                df[f'{g_attr}_binned'] = pd.cut(df[g_attr], bins=bins)
                g_attr = f'{g_attr}_binned'
            gb = GroupBy([g_attr], {g_attr: 'count'})
            # i = OutstandingInsight.create_insight_object(df, None, gb)
            # i.score()
            insights.append(i.insight_json())
            # print(i.insight_json())
        return insights
    
    def create_insight_object(self, df, filter, gb):
         return TrendInsight.create_insight_object(df, filter, gb)

    def to_continue(self,  dtype, g_attr, ser):
            if (dtype in ['int64', 'float64'] or 'binned' not in g_attr ) and len(ser.value_counts().values) > 10:
                 return True
            else:
                 return False
            #     _, bins = pd.cut(ser, n_bins, retbins=True, duplicates='drop')
            #     df[f'{g_attr}_binned'] = pd.cut(df[g_attr], bins=bins)
            #     g_attr_new = f'{g_attr}_binned')
        