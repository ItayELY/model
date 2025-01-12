import operator
import sys
from insights.attribution_insight import AttributionInsight

import pandas as pd
sys.path.append("..") 
from EDADataFrame import EDADataFrame
from operations.operation import Operation

class BaseContextualize():
    def __init__(self, df, insight):
        self.df = df
        self.insight = insight
        pass
