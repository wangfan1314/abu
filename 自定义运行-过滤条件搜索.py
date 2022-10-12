def bigquant_run(bq_graph, inputs):
    candidate = ['return_0>-100']
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

    parameters_list = [{'parameters': {'m10.expr': '&'.join(candidate[:])}}]
    for feature in factor_pool:
        if feature in candidate:
            print('feature exits', feature)
            continue
        tmp = candidate[:]
        tmp.append(feature)
        parameters = {'m10.expr': '&'.join(tmp)}
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
import time

pd.set_option('expand_frame_repr', False)  # 当列太多时显示不清楚
pd.set_option("display.max_rows", 1000)  # 设定显示最大的行数
pd.set_option('max_colwidth', 15)  # 列长度
df_empty = pd.DataFrame()  # 创建一个空的dataframe
file_name = '过滤条件批量测试.csv'
columns = ['时间', '总收益', '最大回撤', 'alpha', '夏普比率', '过滤条件']
for k in range(len(m24.result[0])):
    try:
        # 这里我们要先把·结果读取出来
        cond1 = m24.result[k]['m19'].read_raw_perf()[
            ['starting_value', 'algorithm_period_return', 'alpha', 'beta', 'max_drawdown', 'sharpe']]
        res_tmp = pd.DataFrame(cond1.iloc[-1]).T
        dt = time.strftime('%Y:%m:%d %H:%M:%S', time.localtime(int(time.time())))
        res_tmp['starting_value'] = [dt]
        print('=======')
        expr = m24.result[1][k]
        print('expr:', expr)
        res_tmp['expr'] = [expr]

        res_tmp.rename(columns={'starting_value': '时间',
                                'algorithm_period_return': '总收益',
                                'alpha': 'alpha',
                                'max_drawdown': '最大回撤',
                                'sharpe': '夏普比率',
                                'expr': '过滤条件'}, inplace=True)
        df_empty = pd.DataFrame(res_tmp, columns=columns)
        try:
            Result = pd.read_csv(file_name, index_col=0)
            df_empty.to_csv(file_name, header=False, mode='a', index=False)
        except Exception as e:
            df_empty.to_csv(file_name, header=columns, mode='a', index=False)
        print('写入完成第{}组因子'.format(k))
    except:
        print('第{}组因子出错!请检查'.format(k))
        continue

df = pd.read_csv(file_name)
df.sort_values('夏普比率', inplace=True, ascending=False)
df.to_csv(file_name, header=columns, mode='w', index=False)
print('csv追加写入结束')
