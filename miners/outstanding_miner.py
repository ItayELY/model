import sys
sys.path.append("..") 

from EDADataFrame import EDADataFrame
from operations.filter import Filter
from operations.group_by import GroupBy
from insights.attribution_insight import AttributionInsight
from insights.contextualization import ContextualizedInsight
from insights.outstanding_insight import OutstandingInsight

from miners.insight_miner import Miner
import json
import pandas as pd
import warnings
warnings.filterwarnings("ignore")

class OutstandingMiner(Miner):
    def __init__(self, df: EDADataFrame, group_attrs: list | None, agg_attrs: list | None) -> None:
        super().__init__(df, group_attrs, agg_attrs)
        self.sig = 0.2
        self.imp = 0.8
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
            i = OutstandingInsight.create_insight_object(df, None, gb)
            i.score()
            insights.append(i.insight_json())
            # print(i.insight_json())
        return insights
    def to_continue(self,  dtype, g_attr, ser):
            return True
            
            # if (dtype in ['int64', 'float64'] or 'binned' not in g_attr ) and len(ser.value_counts().values) > 10:
            #     _, bins = pd.cut(ser, n_bins, retbins=True, duplicates='drop')
            #     df[f'{g_attr}_binned'] = pd.cut(df[g_attr], bins=bins)
            #     g_attr_new = f'{g_attr}_binned')
        
    def create_insight_object(self, df, filter, gb):
         return OutstandingInsight.create_insight_object(df, filter, gb)