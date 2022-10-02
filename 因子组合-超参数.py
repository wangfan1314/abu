import numpy as np
import pandas as pd

pd.set_option('expand_frame_repr', False)  # 当列太多时显示不清楚
pd.set_option("display.max_rows", 1000)  # 设定显示最大的行数
pd.set_option('max_colwidth', 15)  # 列长度
df_empty = pd.DataFrame()  # 创建一个空的dataframe
for k in range(len(m31.result)):
    try:
        # 这里我们要先把·结果读取出来
        cond1 = m31.result[k]['m19'].read_raw_perf()[
            ['algorithm_period_return', 'alpha', 'beta', 'max_drawdown', 'sharpe']]
        res_tmp = pd.DataFrame(cond1.iloc[-1]).T
        feature = m31.result[k]['m4'].data.read()
        feature2 = m31.result[k]['m4'].data.read()
        res_tmp['feature'] = [feature2]
        res_tmp['feature_num'] = len(feature2)
        res_tmp['add_feature_name'] = [feature]

        res_tmp.rename(columns={'algorithm_period_return': '总收益',
                                'alpha': 'alpha',
                                'max_drawdown': '最大回撤',
                                'sharpe': '夏普比率',
                                'feature': '因子组合',
                                'add_feature_name': '新增因子',
                                'feature_num': '因子数', }, inplace=True)
        df_empty = pd.DataFrame(res_tmp, columns=['总收益', '最大回撤', 'alpha', '夏普比率', '因子组合', '新増因子', '因子数'])
        df_empty.to_csv('因子test11组批量测试.csv', header=['总收益', '最大回撤', 'alpha', '夏普比率', '因子组合', '新増因子', '因子数'], mode='a')
        print('写入完成第{}组因子'.format(k))
    except:
        print('第{}组因子出错!请检查'.format(k))
        continue

print('csv追加写入结束')
