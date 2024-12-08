import operator
import sys

import pandas as pd
sys.path.append("..") 
from EDADataFrame import EDADataFrame
from operations.operation import Operation


operators = {
    "==": (operator.eq),
    ">": operator.gt,
    ">=": operator.ge,
    "<": operator.lt,
    "<=": operator.le,
    "!=": operator.ne,
    "between": lambda x, tup: x.apply(lambda item: tup[0] <= item < tup[1])
}
def do_operation(a, b, op_str):
    if op_str == 'between':
        pass
    return operators[op_str](a, b)


class FiltersDis(Operation):
    def __init__(self, filters):
        super().__init__()
        # self.operation_str = operation_str
        # self.value = value
        # self.attribute = attribute
        self.filters = filters
        self.values = []
        for f in filters:
            self.values.append(f.value)
        self.type = 'filters disjunction'
    def do_operation(self, df):
        dfs = []
        for f in self.filters:
            dfs.append(f.do_operation(df))
        new_df = pd.concat(dfs)
        return new_df
    def do_operation_not(self, df):
        return EDADataFrame(df.loc[operators[self.operation_str](df[self.attribute], self.value)], operation = self, prev_df=df)
    def __str__(self):
        string = f'{self.filters[0]}'
        for f in self.filters[1:]:
            string = string + f'\nOR\n{str(f)}'  
        return string
    def dict(self):
        i = 1
        dict_list = []
        for f in self.filters:
            dict_list.append([f.dict()])
            i += 1
        return dict_list

