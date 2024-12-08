import operator
import sys
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


class MultiFilter(Operation):
    def __init__(self, filters):
        super().__init__()
        # self.operation_str = operation_str
        # self.value = value
        # self.attribute = attribute
        self.filters = filters
        self.values = []
        for f in filters:
            self.values.append(f.value)
        self.type = 'multi filter'
        # if result_df is None:
        #     self.operation_str = operation_str
        #     self.value = value
        #     self.result_df = self.source_df[do_operation(self.source_df[attribute], value, operation_str)]
        # else:
        #     self.result_df = result_df
        #     self.result_name = utils.get_calling_params_name(result_df)
        #     # result_df.name = self.result_name
        # self.source_name = utils.get_calling_params_name(source_df)
        # source_df.name = self.source_name
    def do_operation(self, df):
        new_df = df
        for f in self.filters:
            new_df = f.do_operation(new_df)
        return new_df
    def do_operation_not(self, df):
        return EDADataFrame(df.loc[operators[self.operation_str](df[self.attribute], self.value)], operation = self, prev_df=df)
    def __str__(self):
        string = ''
        for f in self.filters:
            string = string + str(f)
        return string
    def dict(self):
        i = 1
        dict_list = []
        for f in self.filters:
            dict_list.append([f.dict()])
            i += 1
        return dict_list

