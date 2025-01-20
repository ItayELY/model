from pandas._typing import Level, Renamer, IndexLabel, Axes, Dtype

import numpy as np
import pandas as pd
from matplotlib.axis import Axis
from pandas._libs.lib import no_default
from operations.operation import Operation

class EDADataFrame(pd.DataFrame):
    def __init__(
            self,
            data=None,
            index: Axes | None = None,
            columns: Axes | None = None,
            dtype: Dtype | None = None,
            copy: bool | None = None,
            operation: Operation | None = None,
            prev_df: pd.DataFrame = None
    ):
        super().__init__(data, index, columns, dtype, copy)
        self.operation = operation
        self.filter_items = None
        self.prev_df = prev_df
    
    def get_operation(self):
        return self.operation
    
    def get_root(self):
        if self.prev_df is None:
            return self
        return self.prev_df.get_root()
    def get_path(self):
        if self.prev_df is None:
            return ' '
        path = self.prev_df.get_path()
        path += f"{self.operation}, "
        return path
    
    def get_retreival_query(self):
          if self.prev_df is None:
            return []
          lst = self.prev_df.get_retreival_query()
          return lst + [self.operation]
    
    def get_retreival_query_readable(self):
          if self.prev_df is None:
            return []
          lst = self.prev_df.get_retreival_query()
          return lst + [{f'{self.operation.type}': self.operation.dict() }]

    
    ## overload pandas
    def copy(self, deep=True):
        """
        Make a copy of this object’s indices and data.
        :param deep: Make a deep copy, including a copy of the data and the indices.
                     With deep=False neither the indices nor the data are copied.
        :return: explain dataframe copy
        """
        return EDADataFrame(super().copy(deep))
        
