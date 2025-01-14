import operator
import sys
sys.path.append("..") 
from EDADataFrame import EDADataFrame
from operations.operation import Operation

def not_op(op):
    d = {'==': '!=', '!=': '==', '<': '>', '>': '<'}
    return d[op]
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


class Filter(Operation):
    def __init__(self, attribute, operation_str, value):
        super().__init__()
        self.operation_str = operation_str
        self.value = value
        self.attribute = attribute
        self.type = 'filter'
        self.id= 'F' + attribute + operation_str + str(value)
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
        return EDADataFrame(df.loc[operators[self.operation_str](df[self.attribute], self.value)], operation = self, prev_df=df)
    def do_operation_not(self, df):
        return EDADataFrame(df.loc[operators[not_op(self.operation_str)](df[self.attribute], self.value)], operation = self, prev_df=df)
    def __str__(self):
        if self.operation_str == 'between':
            return f"{self.value[0]} < {self.attribute} < {self.value[1]}"
        return (f'"{self.attribute} {self.operation_str} {self.value}"')
    def dict(self):
        return {
            'attribute': self.attribute,
            'operator': self.operation_str,
            'value': self.value
            }

