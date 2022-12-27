def bigquant_run(bq_graph, inputs):
    batch_num = 4  # 多少20组,需要跑多少组策略100
    factor_last = list()  # 做一个空列表储存已经测试过的因子
    try:
        Result = pd.read_csv('因子st批量测试.csv', index_col=0)
        Result_feature = list(Result['因子组合'])
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
                   'fs_cash_ratio_0',
                   'close_0>ts_max(close_0,56)',
                   'ta_sma_10_0/ta_sma_30_0',
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
                   'pl1=sum(price_limit_status_0==3,80)',
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
                   'mean(close_0,3)/close_0',
                   'avg_amount_0/avg_amount_5',
                   'avg_amount_5/avg_amount_20',
                   'rank_avg_amount_0/rank_avg_amount_5',
                   'rank_avg_amount_5/rank_avg_amount_10',
                   'rank_return_0',
                   'rank_return_5',
                   'rank_return_10',
                   'rank_return_0/rank_return_5',
                   'rank_return_5/rank_return_10',
                   'amount_0/amount_1',
                   'high_0/close_1',
                   'hpbl10=ts_max(close_0,10)/ts_min(close_0,10)',
                   'lxsz_days=max(where(sum(where(return_0>1,1,0),1)==1,1,0),where(sum(where(return_0>1,1,0),2)==2,2,0),where(sum(where(return_0>1,1,0),3)==3,3,0),where(sum(where(return_0>1,1,0),4)==4,4,0),where(sum(where(return_0>1,1,0),5)==5,5,0),where(sum(where(return_0>1,1,0),6)==6,6,0),where(sum(where(return_0>1,1,0),8)==8,8,0),where(sum(where(return_0>1,1,0),10)==10,10,0))',
                   'lxxd_days=max(where(sum(where(return_0<1,1,0),1)==1,1,0),where(sum(where(return_0<1,1,0),2)==2,2,0),where(sum(where(return_0<1,1,0),3)==3,3,0),where(sum(where(return_0<1,1,0),4)==4,4,0),where(sum(where(return_0<1,1,0),5)==5,5,0),where(sum(where(return_0<1,1,0),6)==6,6,0),where(sum(where(return_0<1,1,0),8)==8,8,0),where(sum(where(return_0<1,1,0),10)==10,10,0))',
                   'ta_trix(close_0)']

    hf_factor_pool = ['hf_real_var',
                      'hf_real_kurtosis',
                      'hf_real_skew',
                      'hf_real_std',
                      'hf_real_upvar',
                      'hf_real_downvar',
                      'hf_real_upstd',
                      'hf_real_downstd',
                      'hf_ratio_realupstd',
                      'hf_ratio_realdownstd',
                      'hf_real_var_3',
                      'hf_real_kurtosis_3',
                      'hf_real_skew_3',
                      'hf_real_std_3',
                      'hf_real_var_6',
                      'hf_real_kurtosis_6',
                      'hf_real_skew_6',
                      'hf_real_std_6',
                      'hf_volume_var',
                      'hf_volume_kurtosis',
                      'hf_volume_skew',
                      'hf_volume_std',
                      'hf_ratio_volume_3',
                      'hf_ratio_volume_5',
                      'hf_corr_vp_3',
                      'hf_corr_vp_4',
                      'hf_corr_vp_5',
                      'hf_corr_vp_6',
                      'hf_ret_in_3',
                      'hf_ret_in_5',
                      'hf_ret_in_6',
                      'hf_trend_str_3',
                      'hf_trend_str_5',
                      'hf_trend_str_6',
                      'hf_volume_ratio_stag e1',
                      'hf_volume_ratio_stag e2',
                      'hf_cross_night_chg',
                      'hf_price_chg_stage1',
                      'hf_price_chg_stage2',
                      'hf_price_range_stage 1',
                      'hf_price_range_stage 2',
                      'hf_volume_auction_a m_ratio',
                      'hf_amount_auction_a m_ratio',
                      'hf_open_max_cng_sta ge1',
                      'hf_open_min_cng_sta ge1',
                      'hf_open_max_cng_sta ge2',
                      'hf_open_min_cng_sta ge2',
                      'hf_last_price_stage1',
                      'hf_last_price_stage2',
                      'hf_price_increase_sta ge2',
                      'hf_price_decrease_sta ge2',
                      'hf_price_avg_stage2',
                      'hf_price_max_stage2',
                      'hf_price_min_stage2',
                      'hf_price_chg_abs_sta ge2',
                      'hf_price_pct_chg_stag e2',
                      'hf_continuous_volum e_ratio',
                      'hf_tot_volume_bid',
                      'hf_open_volume_bid',
                      'hf_close_volume_bid',
                      'hf_tot_volume_ask',
                      'hf_open_volume_ask',
                      'hf_close_volume_ask',
                      'hf_open_buy_volume_exlarge_order',
                      'hf_close_buy_volume_exlarge_order',
                      'hf_open_sell_volume_ exlarge_order',
                      'hf_close_sell_volume_ exlarge_order',
                      'hf_open_buy_volume_exlarge_order_act',
                      'hf_close_buy_volume_exlarge_order_act',
                      'hf_open_sell_volume_ exlarge_order_act',
                      'hf_close_sell_volume_ exlarge_order_act',
                      'hf_open_netinflow_ra te_exlarge_order_act',
                      'hf_close_netinflow_ra te_exlarge_order_act',
                      'hf_net_inflow_value_r ate',
                      'hf_open_net_inflow_v alue_rate',
                      'hf_close_net_inflow_v alue_rate']
    batch_factor = list()
    for i in range(batch_num):
        # factor_num = 2  # 每组多少个因子
        factor_num = random.randint(6, 20)
        fatchors = random.sample(factor_pool, factor_num)
        hf_factor_num = random.randint(5, 10)
        hf_fatchors = random.sample(hf_factor_pool, hf_factor_num)

        batch_factor.append(fatchors + hf_fatchors)

    parameters_list = []
    feature_list = []
    for feature in batch_factor:
        if str(feature) in factor_last:
            print('continue111222')
            continue
        factor_last.append(feature)
        # Result['因子数'] = len(feature)  # 这里计数总共有测试了多少个因子
        # Result['新增因子'] = [feature]  # 这里记录新测试的是哪个因子
        # Result.to_csv('因子表.csv', header=['新增因子', '因子数'], mode='a')  # 把测试好的因子追加写入因子表
        feature_list.append(feature)
        parameters = {'m15.features': '\n'.join(feature)}
        parameters_list.append({'parameters': parameters})

    def run(parameters):
        try:
            return g.run(parameters)
        except Exception as e:
            print('ERROR-----------', e)
            return None

    print('factor_last', len(factor_last))
    print('feature_list', len(feature_list))
    results = T.parallel_map(run, parameters_list, max_workers=4, remote_run=False, silent=True)  # 任务数 # 是否远程#
    return results, feature_list


import numpy as np
import pandas as pd
import time

pd.set_option('expand_frame_repr', False)  # 当列太多时显示不清楚
pd.set_option('display.max_rows', 1000)  # 设定显示最大的行数
pd.set_option('max_colwidth', 15)  # 列长度
df_empty = pd.DataFrame()  # 创建一个空的dataframe
file_name = '因子st批量测试.csv'
columns = ['时间', '总收益', 'alpha', '最大回撤', '夏普比率', '2021总收益', '2021alpha', '2021最大回撤', '2021夏普比率', '因子组合', '因子数']
for k in range(len(m35.result[0])):
    # for k in range(1):
    try:
        # 这里我们要先把·结果读取出来
        feature = m35.result[1][k]
        # print(feature)
        print('=======')
        cond1 = m35.result[0][k]['m14'].read_raw_perf()[
            ['starting_value', 'algorithm_period_return', 'alpha', 'beta', 'max_drawdown', 'sharpe']]
        res_tmp = pd.DataFrame(cond1.iloc[-1]).T
        dt = time.strftime('%Y:%m:%d %H:%M:%S', time.localtime(int(time.time())))
        res_tmp['starting_value'] = [dt]
        res_tmp['feature'] = [feature]
        res_tmp['feature_num'] = len(feature)
        res_tmp['2021总收益'] = ''
        res_tmp['2021alpha'] = ''
        res_tmp['2021最大回撤'] = ''
        res_tmp['2021夏普比率'] = ''

        res_tmp.rename(columns={'starting_value': '时间',
                                'algorithm_period_return': '总收益',
                                'alpha': 'alpha',
                                'max_drawdown': '最大回撤',
                                'sharpe': '夏普比率',
                                '2021总收益': '2021总收益',
                                '2021alpha': '2021alpha',
                                '2021最大回撤': '2021最大回撤',
                                '2021夏普比率': '2021夏普比率',
                                'feature': '因子组合',
                                'feature_num': '因子数', }, inplace=True)
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

from datetime import datetime

print(datetime.now())
print(len(m35.result[0]))
for i in range(len(m35.result[0])):
    try:
        print('===', i)
        perf = m35.result[0][i]['m14'].raw_perf.read()
        re = perf['algorithm_period_return'].iloc[-1]
        days = perf['trading_days'].iloc[-1]
        annual_re = (math.pow(1 + re, 252 / days) - 1) * 100
        sharpe = round(perf['sharpe'].iloc[-1], 2)
        if annual_re <= 0 or perf['max_drawdown'].iloc[-1] * 100 < -20 or sharpe < 2:
            continue
        feature = m35.result[1][i]
        print(feature)
        print("年化收益：{}%".format(round(annual_re, 2)))
        print("收益率：{}%".format(round(perf['algorithm_period_return'].iloc[-1] * 100, 2)))
        print("基准收益率：{}%".format(round(perf['benchmark_period_return'].iloc[-1] * 100, 2)))
        print("阿尔法：{}".format(round(perf['alpha'].iloc[-1], 2)))
        print("贝塔：{}".format(round(perf['beta'].iloc[-1], 2)))
        print("夏普比率：{}".format(sharpe))
        print("胜率：{}".format(round(perf['win_percent'].iloc[-1], 2)))
        print("收益波动率：{}%".format(round(perf['algo_volatility'].iloc[-1] * 100, 2)))
        print("信息比率：{}".format(round(perf['information'].iloc[-1], 2)))
        print("最大回撤：{}%".format(round(perf['max_drawdown'].iloc[-1] * 100, 2)))

        T.plot(perf['algorithm_period_return'], title='收益', chart_type='line')

    except Exception as e:
        print(e)
        continue
