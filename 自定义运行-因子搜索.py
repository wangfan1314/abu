def bigquant_run(bq_graph, inputs):
    factor_last = list()  # 做一个空列表储存已经测试过的因子
    try:
        Result = pd.read_csv("因子test11组批量测试.csv", index_col=0)
        Result_feature = list(Result["因子组合"])
        for done in Result_feature:
            if done == '因子组合':
                continue
            factor_last.append(done)
    except Exception as e:
        print('ERROR-----------', e)
    # print('factor_last', factor_last)
    factor_pool = ['return_5/return_20',
                   'rank_amount_5',
                   'avg_turn_10',
                   'pe_ttm_0',
                   'pb_lf_0',
                   'sum(mf_net_pct_main_0>0.12,30)',
                   'fs_roa_ttm_0',
                   'fs_cash_ratio_0',
                   'close_0>ts_max(close_0,56)',
                   'ta_sma_10_0/ta_sma_30_0#',
                   'ta_sar_0',
                   'swing_volatility_10_0/swing_volatility_60_0',
                   'ta_cci_14_0',
                   'rank_return_3',
                   'mf_net_amount_0>mf_net_amount_1',
                   'mf_net_amount_xl_0>mean(mf_net_amount_xl_0, 30)',
                   '(close_0-close_1)/close_1',
                   '(close_0-close_30)/close_30',
                   '(close_0-close_5)/close_5',
                   'ta_bbands_middleband_28_0',
                   'sum(price_limit_status_0==3,80)',
                   'close_0',
                   'open_0',
                   'high_0',
                   'low_0 ',
                   'amount_0',
                   'turn_0 ',
                   'return_0',
                   'high_1',
                   'return_1',
                   'amount_1',
                   'turn_1',
                   'open_2',
                   'low_2',
                   'amount_2',
                   'turn_2',
                   'return_2',
                   'close_3',
                   'amount_3',
                   'turn_3',
                   'return_3',
                   'high_4',
                   'low_4',
                   'amount_4',
                   'turn_4',
                   'return_4',
                   'log(sum(return_0, 8))',
                   'delta(return_3, 4)',
                   'div(min(return_0, swing_volatility_5_0), log(rank_return_0))',
                   'log(swing_volatility_5_0)',
                   'div(return_0, return_3)',
                   'sub(swing_volatility_5_0, rank_return_0)',
                   'sub(normalize(return_3), div(rank_return_3, turn_3))',
                   'sub(normalize(normalize(return_0)), rank_turn_5)',
                   'sub(rank_turn_5, rank_return_0)',
                   'add(swing_volatility_5_0, return_0)',
                   'add(return_0, return_3)',
                   'decay_linear(return_0, 1)',
                   'sub(return_0, swing_volatility_5_0)',
                   'volatility_5_0',
                   'std(volume_0,5)',
                   'std(deal_number_0,5)',
                   'avg_turn_5',
                   'std(turn_0,5)',
                   'std(amount_0,5)',
                   'std(return_0-1,5)',
                   '((-1*rank(std(high_0,10)))*correlation(high_0,volume_0,10))',
                   '(-1*rank(covariance(rank(high_0),rank(volume_0),5)))',
                   '(-1*rank(covariance(rank(close_0),rank(volume_0),5)))',
                   '(-1*ts_max(rank(correlation(rank(volume_0),rank((high_0+low_0+close_0+open_0)/4),5)),5))',
                   '(-1*ts_max(rank(correlation(rank(volume_0),rank(((high_0+low_0+open_0+close_0)*0.25)),5)),5))',
                   '(rank(correlation(rank((high_0+low_0+close_0+open_0)/4),rank(volume_0),5))*-1)',
                   '(-1*correlation(high_0,rank(volume_0),5))',
                   '(-1*sum(rank(correlation(rank(high_0),rank(volume_0),3)),3))',
                   '(rank(((high_0+low_0+close_0+open_0)/4-close_0))/rank(((high_0+low_0+close_0+open_0)/4+close_0)))',
                   '(-1*correlation(rank(open_0),rank(volume_0),10))',
                   '(-1*ts_max(correlation(ts_rank(volume_0,5),ts_rank(high_0,5),5),3))',
                   '(-1*correlation(open_0,volume_0,10))',
                   '(-1*rank(((std(abs((close_0-open_0)),5)+(close_0-open_0))+correlation(close_0,open_0,10))))',
                   '(-1*volume_0/mean(volume_0,20))',
                   '(-1*correlation(rank((close_0-ts_min(low_0,12))/(ts_max(high_0,12)-ts_min(low_0,12))),rank(volume_0),6))',
                   '((-1*rank(delta(return_0,3)))*correlation(open_0,volume_0,10))',
                   '((-1*rank(delta((close_0/shift(close_0,1)-1),3)))*correlation(open_0,volume_0,10))',
                   '(sign(delta(volume_0,1))*(-1*delta(close_0,1)))',
                   '((rank(delay(((high_0-low_0)/(sum(close_0,5)/5)),2))*rank(rank(volume_0)))/(((high_0-low_0)/(sum(close_0,5)/5))/((high_0+low_0+close_0+open_0)/4-close_0)))',
                   'fs_total_equity_0/market_cap_0',
                   'mean(close_0,3)/close_0']

    batch_num = 8  # 多少20组,需要跑多少组策略100
    batch_factor = list()
    for i in range(batch_num):
        # factor_num = 2  # 每组多少个因子
        factor_num = random.randint(8, 15)
        batch_factor.append(random.sample(factor_pool, factor_num))

    # print('batch_factor', batch_factor)
    parameters_list = []
    for feature in batch_factor:
        if str(feature) in factor_last:
            print("continue111222")
            continue
        factor_last.append(feature)
        # Result['因子数'] = len(feature)  # 这里计数总共有测试了多少个因子
        # Result['新增因子'] = [feature]  # 这里记录新测试的是哪个因子
        # Result.to_csv('因子表.csv', header=['新增因子', '因子数'], mode='a')  # 把测试好的因子追加写入因子表
        parameters = {'m4.features': '\n'.join(feature)}
        parameters_list.append({'parameters': parameters})

    def run(parameters):
        try:
            return g.run(parameters)
        except Exception as e:
            print('ERROR-----------', e)
            return None

    print('parameters_list', parameters_list)
    results = T.parallel_map(run, parameters_list, max_workers=4, remote_run=False, silent=True)  # 任务数 # 是否远程#
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
        print('feature:', feature)
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
