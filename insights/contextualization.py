import sys
sys.path.append("..") 

from EDADataFrame import EDADataFrame
from insights.base_insight import BaseInsight
from operations.filter import Filter
from operations.group_by import GroupBy
import operator

opposite_ops = {
    "==": "!=",
    ">": "<=",
    ">=": "<",
    "<": ">=",
    "<=": ">",
    "!=": "==",
    "between": lambda x, tup: x.apply(lambda item: tup[0] > item >= tup[1])
}
class ContextualizedInsight():
    def __init__(self, insight: BaseInsight, ref_df: EDADataFrame | None = None, cnx_type: str | None = None, 
                 ):
        self._insight = insight
        self.target_df = insight._df
        self.rq_changes = []
        if ref_df is None:
            self.ref_df = self.ommit_first_filter_reference(self.target_df)
            self.contextualize()
        else:
            self.ref_df = ref_df
        self.contextualization = {
            "cnx_type": cnx_type
        }

    def compare_ref(self, ref_df):
        score1 = self._insight.score(self.target_df)
        score2 = self._insight.score(ref_df)

        if abs(score1/(score2 + 0.01) - 1) < 0.5:
            return 1
        return 2
    
    def ommit_first_filter_reference(self, df: EDADataFrame):
        if df.prev_df is None:
            return None
        ops = []
        tmp_df = df
        # op = df.get_operation()
        while tmp_df.prev_df is not None:
            op = tmp_df.get_operation()
            ops = [op] + ops
            tmp_df = tmp_df.prev_df
        # new_op = Filter(ops[0].attribute, opposite_ops[ops[0].operation_str], ops[0].value)
        # ref_df = new_op.do_operation(tmp_df)
        for oper in ops[1:]:
            tmp_df = oper.do_operation(tmp_df)
        rq_change = {'omitted': str(ops[0])}
        self.rq_changes += [rq_change]
        return tmp_df


    def basic_backtrack_reference(self, df: EDADataFrame):
        if df.prev_df is None:
            return None
        op = df.get_operation()
        if op.type == 'filter':
            new_op = Filter(op.attribute, opposite_ops[op.operation_str], op.value)
            ref_df = new_op.do_operation(df.prev_df)
            rq_change = {'before': str(op), 'after': str(new_op)}
            self.rq_changes += [rq_change]
            return ref_df
        return None

    def contextualize(self):
        if self.ref_df is None:
            # print('No Contextualization!')
            self.contextualization = None
            return
        if self.compare_ref(self.ref_df) == 1:
            sim_dif = 'similar'
        else:
            sim_dif = 'different'
        self.contextualization = {
            'retreival_query_changes':self.rq_changes,
            'reference_type': sim_dif
        }
        # print(f"Replacing {str(self.target_df.operation)} with {str(self.ref_df.operation)} would give a {sim_dif} result:")
        # print(str(self._insight.get_insight_view()))
    def cnx_json(self):
        return self.contextualization
        