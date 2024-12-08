import operator
import sys
sys.path.append("..") 
from EDADataFrame import EDADataFrame
from operations.operation import Operation


class GroupBy(Operation):
    def __init__(self, group_attributes, agg_dict):
        super().__init__()
        self.group_attributes = group_attributes
        self.agg_dict = agg_dict
        self.type = 'groupby'
        self.id= 'GB' + str(group_attributes) + str(agg_dict)
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
        new_df = df.groupby(self.group_attributes)#.agg(self.agg_dict)
        new_df = new_df.agg(self.agg_dict)
        t = type(new_df)
        return EDADataFrame(new_df, operation=self, prev_df=df)
    def __str__(self):
        return f'Group By {self.group_attributes}, Select {self.agg_dict}'
    def dict(self):
        return {
            'grouping_attribute': self.group_attributes[0],
            'aggregation_attribute': list(self.agg_dict.items())[0][0],
            'aggregation_method': list(self.agg_dict.items())[0][1]
        }
        
