import operator
import sys

from matplotlib import pyplot as plt
import numpy as np

sys.path.append("..") 
from operations.filter import Filter
from operations.group_by import GroupBy
from insights.outstanding_insight import OutstandingInsight
from insights.base_insight import BaseInsight
from EDADataFrame import EDADataFrame
from final_contextualize.base_contextualize import BaseContextualize
from miners.insight_miner import Miner
from final_contextualize.global_contextualize import GlobalContextualize

class ContextualizedInsight():
    def __init__(self, insight: BaseInsight):
        self._insight = insight
        self.target_df = insight._df
        self.target_rq = self.target_df.get_retreival_query()
        self.ctx = GlobalContextualize(self.target_df, self._insight)

    def contextualize(self):
        level = 2
        self.ctx.contextualize(level)
        op = self.target_rq[-level]

        if len(self.ctx.sim_insights) > 0:
            self.sim_insight = self.ctx.sim_insights[0]
            self.sim_df = self.sim_insight[0]._df
            self.sim_op = self.sim_df.get_retreival_query()[-level]
        else:
            self.sim_insight = None

        if len(self.ctx.dist_insights) > 0:
            self.dist_insight = self.ctx.dist_insights[0]
            self.dist_df = self.dist_insight[0]._df
            self.dist_op = self.dist_df.get_retreival_query()[-level]
        else:
            self.dist_insight = None

        pass

    def show_ctx_insight(self):
        to_plot = []
        # if self.sim_insight:
        #     to_plot.append(('similar', self.sim_insight))
        if self.dist_insight:
            to_plot.append(('distinct', self.dist_insight))

        fig, axes = plt.subplots(2 + len(to_plot), layout = 'constrained', figsize = (7,14 + 7*len(to_plot)))
        ax_idx = 0
        highlight = self._insight.highlight
        insight_view = self._insight.get_insight_view(self.target_df)
    # insight_ref = ins.create_insight_object(df_ref, ins.filter, ins.group_by_aggregate)
        # insight_view_ref = ins.get_insight_view(df_ref)
    # idx = set(list(str(i) for i in insight_view.index) + list(str(i) for i in insight_view_ref.index))
        idx = set(list(i for i in insight_view.index))

        for i in idx:
            if i not in list((i) for i in insight_view.index):
                insight_view.loc[i] = 0
            # if i not in list((i) for i in insight_view_ref.index):
            #     insight_view_ref.loc[i] = 0
    
        vals = list(insight_view.loc[i][0] for i in idx)
        # vals_ref = list(insight_view_ref.loc[i][0] for i in idx)
        colors = []
        colors_ref = []
        for i in idx:
            if i == (highlight):
                colors.append('blue')
                # colors_ref.append(color_other)
            else:
                colors.append('cornflowerblue')
                # colors_ref.append(color_other)
    # colors = ['blue' for i in idx if i != str(i.highlight) else 'orange']  # Highlight the second bar
        X_axis = np.arange(len(idx))
        axes[ax_idx].bar(X_axis, vals, 0.4, color=colors, align="center")
        # ax.bar(X_axis + 0.2, vals_ref, 0.4, color=colors_ref, align="center")
        title = f'{self._insight.show_insight()}'
        

    
    
    
        ticklabels = [str(i) for i in idx]
        axes[ax_idx].set_xticks(X_axis)
        axes[ax_idx].set_xticklabels(ticklabels)
        axes[ax_idx].set_title(title)









        ax_idx += 1
        if ax_idx > len(to_plot):
            plt.show()
            return
        ins = to_plot[ax_idx-1][1][0]
        insight_view_c = ins.get_insight_view(ins._df)
    # insight_ref = ins.create_insight_object(df_ref, ins.filter, ins.group_by_aggregate)
        # insight_view_ref = ins.get_insight_view(df_ref)
    # idx = set(list(str(i) for i in insight_view.index) + list(str(i) for i in insight_view_ref.index))
        # idx = set(list(i for i in insight_view_c.index))

        for i in idx:
            if i not in list((i) for i in insight_view_c.index):
                insight_view_c.loc[i] = 0
            # if i not in list((i) for i in insight_view_ref.index):
            #     insight_view_ref.loc[i] = 0
    
        vals = list(insight_view_c.loc[i][0] for i in idx)
        # vals_ref = list(insight_view_ref.loc[i][0] for i in idx)
        colors = []
        colors_ref = []
        for i in idx:
            if i == (highlight):
                colors.append('blue')
                # colors_ref.append(color_other)
            else:
                colors.append('cornflowerblue')
                # colors_ref.append(color_other)
    # colors = ['blue' for i in idx if i != str(i.highlight) else 'orange']  # Highlight the second bar
        X_axis = np.arange(len(idx))
        axes[ax_idx].bar(X_axis, vals, 0.4, color=colors, align="center")
        # ax.bar(X_axis + 0.2, vals_ref, 0.4, color=colors_ref, align="center")
        title = f'{ins.show_insight()}'
        

    
    
    
        ticklabels = [str(i) for i in idx]
        axes[ax_idx].set_xticks(X_axis)
        axes[ax_idx].set_xticklabels(ticklabels)
        axes[ax_idx].set_title(title)

        ax_idx += 1

        try:
            axes[ax_idx].hist(ins._df[self.dist_op.attribute], color='green', label='reference DF')
        except:
            pass

        axes[ax_idx].hist(self.target_df[self.dist_op.attribute], color='blue', label='your DF')
        axes[ax_idx].set_title('distributions of the altered attribute\nin your and the reference DFs')
        # try:
        #     inter = filter1.do_operation_not(all_songs)
        #     fin = f2.do_operation(inter)
        #     axes[2].hist(fin[attr], color='orange', label='distinct')
        # except: pass
        plt.legend()
        plt.show()
    # ax.set_xlabel(f"{ins.group_by_aggregate.group_attributes[0]}")
    # agg_item = list(ins.group_by_aggregate.agg_dict.items())[0]
    # ax.set_ylabel(f"{agg_item[1]} of {agg_item[0]}")
        
# 
    # ax.set_xticklabels(ticklabels)
    # ax.tick_params(axis='x', labelrotation=50)




#     dist_insight = ctx.dist_insights[0]
#     axes[1] = viz_insight_with_ref(df2, dist_insight[0]._df, insight_1[1], axes[1], 'distinct', insight_1[1].highlight, color_other='orange')
#     f2 = dist_insight[1]
# attr = 'year'
# try:
#     axes[2].hist(f1.do_operation(all_songs)[[attr]], color='green', label='similar')
# except:
#     pass

# axes[2].hist(df2.prev_df[attr], color='blue', label='your DF')
# try:
#     inter = filter1.do_operation_not(all_songs)
#     fin = f2.do_operation(inter)
#     axes[2].hist(fin[attr], color='orange', label='distinct')
# except: pass
# plt.legend()