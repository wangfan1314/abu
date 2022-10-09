def bigquant_run(bq_graph, inputs):
    factor_pool = ['return_0>1.03',
                   'volume_0/volume_1>1.7',
                   'turn_0==max_turn',
                   'ac0==max_ac',
                   'sma_close_5>sma_close_10',
                   'sma_close_5>sma_close_20',
                   'sma_volume_5>sma_volume_10',
                   'sma_volume_5>sma_volume_20',
                   'open_0<close_0',
                   'mf_net_pct_main_0<0.4',
                   'sum_mf>0.1',
                   'rank_return_0>0.9',
                   'rank_return_5>0.9',
                   'rank_avg_mf_net_amount_5<0.5',
                   'shangying<0.04',
                   'xiayingzhu<2']

    parameters_list = [{'parameters': {'m10.expr': '&'.join(factor_pool[:])}}]
    minus_list = ['&'.join(factor_pool[:])]
    for feature in factor_pool:
        tmp = factor_pool[:]
        tmp.remove(feature)
        minus_list.append(feature)
        parameters = {'m10.expr': '&'.join(tmp)}
        parameters_list.append({'parameters': parameters})

    def run(parameters):
        try:
            return g.run(parameters)
        except Exception as e:
            print('ERROR-----------', e)
            return None

    # print("parameters_list:", parameters_list)
    print("minus_list:", minus_list)
    results = T.parallel_map(run, parameters_list, max_workers=2, remote_run=False, silent=False)  # 任务数 # 是否远程#
    return results, minus_list


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
