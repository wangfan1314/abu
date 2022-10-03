def bigquant_run(bq_graph, inputs):
    # Result = pd.read_csv("因子test1组批量测试-排序结果.csv",index_col=0)
    # Result_feature = list(Result["新增因子"])
    factor_last = []  # 做一个空列表储存已经测试过的因子
    factor_pool = ['alpha_10242=-1*swing_volatility_5_0',
                   'correlation (turn_0, return_0,5)',
                   'alpha_10203=-1*(rank(correlation(sum(((low_0*0.35)+((high_0+low_0+open_0+close_0)/4*0.65)),20),sum(mean(volume_0,40),20),7))+rank(correlation(rank((high_0+low_0+open_0+close_0)/4),rank(volume_0),6)))',
                   'return_0',
                   'alpha_10133=std(return_0-1,10)',
                   'alpha_10151=((close_0-sum(min(low_0,delay(close_0,1)),6))/sum(max(high_0,delay(close_0,1))-min(low_0,delay(close_0,1)),6)*12*24+(close_0-sum(min(low_0,delay(close_0,1)),12))/sum(max(high_0,delay(close_0,1))-min(low_0,delay(close_0,1)),12)*6*24+(close_0-sum(min(low_0,delay(close_0,1)),24))/sum(max(high_0,delay(close_0,1))-min(low_0,delay(close_0,1)),24)*6*24)*100/(6*12+6*24+12*24)',
                   'alpha_10406=((rank(delay(((high_0-low_0)/(sum(close_0,5)/5)),2))*rank(rank(volume_0)))/(((high_0-low_0)/(sum(close_0,5)/5))/((high_0+low_0+close_0+open_0)/4-close_0)))',
                   'alpha_10189=-1*((high_0-ta_sma2(close_0,15,2))-(low_0-ta_sma2(close_0,15,2)))/close_0']

    batch_num = 8  # 多少20组,需要跑多少组策略100
    batch_factor = list()
    for i in range(batch_num):
        random.seed(i)
        factor_num = 2  # 每组多少个因子
        batch_factor.append(random.sample(factor_pool, factor_num))

    parameters_list = []
    for feature in batch_factor:
        if feature in factor_last:
            print("continue111222")
            continue
        print(feature)
        factor_last.append(feature)
        # Result['因子数'] = len(feature)  # 这里计数总共有测试了多少个因子
        # Result['新增因子'] = [feature]  # 这里记录新测试的是哪个因子
        # Result.to_csv('因子表.csv', header=['新增因子', '因子数'], mode='a')  # 把测试好的因子追加写入因子表
        parameters = {'m4.features': '\n'.join(feature)}
        print("parameters:", parameters)
        parameters_list.append({'parameters': parameters})

    def run(parameters):
        try:
            return g.run(parameters)
        except Exception as e:
            print('ERROR-----------', e)
            return None

    results = T.parallel_map(run, parameters_list, max_workers=2, remote_run=False, silent=True)  # 任务数 # 是否远程#
    return results



import numpy as np
import pandas as pd

pd.set_option('expand_frame_repr', False)  # 当列太多时显示不清楚
pd.set_option("display.max_rows", 1000)  # 设定显示最大的行数
pd.set_option('max_colwidth', 15)  # 列长度
df_empty = pd.DataFrame()  # 创建一个空的dataframe
for k in range(len(m24.result)):
    try:
        # 这里我们要先把·结果读取出来
        print('=======')
        cond1 = m24.result[k]['m19'].read_raw_perf()[
            ['algorithm_period_return', 'alpha', 'beta', 'max_drawdown', 'sharpe']]
        res_tmp = pd.DataFrame(cond1.iloc[-1]).T
        feature = m24.result[k]['m4'].data.read()
        print('feature:',feature)
        feature2 = m24.result[k]['m4'].data.read()
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
