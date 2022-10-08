def bigquant_run(bq_graph, inputs):
    tree_list = [11, 12, 13, 14, 15, 16, 17, 18, 19, 20]
    # tree_list = [12,13]
    rate_list = [0.05, 0.1]
    # rate_list = [0.1]
    parameters_list = []
    for tree in tree_list:
        for rate in rate_list:
            parameters = {'m6.number_of_trees': tree,'m6.learning_rate': rate}
            parameters_list.append({'parameters': parameters})

    def run(parameters):
        try:
            return g.run(parameters)
        except Exception as e:
            print('ERROR-----------', e)
            return None

    print("parameters_list:", parameters_list)
    results = T.parallel_map(run, parameters_list, max_workers=2, remote_run=False, silent=False)  # 任务数 # 是否远程#
    return results, parameters_list


import numpy as np
import pandas as pd

pd.set_option('expand_frame_repr', False)  # 当列太多时显示不清楚
pd.set_option("display.max_rows", 1000)  # 设定显示最大的行数
pd.set_option('max_colwidth', 15)  # 列长度
df_empty = pd.DataFrame()  # 创建一个空的dataframe
for k in range(len(m24.result[0])):
    try:
        # 这里我们要先把·结果读取出来
        cond1 = m24.result[0][k]['m19'].read_raw_perf()[
            ['algorithm_period_return', 'alpha', 'beta', 'max_drawdown', 'sharpe']]
        res_tmp = pd.DataFrame(cond1.iloc[-1]).T
        print('=======')
        expr = m24.result[1][k]
        print('expr:', expr)
        res_tmp['expr'] = [expr]

        res_tmp.rename(columns={'algorithm_period_return': '总收益',
                                'alpha': 'alpha',
                                'max_drawdown': '最大回撤',
                                'sharpe': '夏普比率',
                                'expr': '过滤条件'}, inplace=True)
        df_empty = pd.DataFrame(res_tmp, columns=['总收益', '最大回撤', 'alpha', '夏普比率', '过滤条件'])
        df_empty.to_csv('过滤条件批量测试.csv', header=['总收益', '最大回撤', 'alpha', '夏普比率', '过滤条件'], mode='a')
        print('写入完成第{}组因子'.format(k))
    except:
        print('第{}组因子出错!请检查'.format(k))
        continue

print('csv追加写入结束')
