from random import random


def bigquant_run(bq_graph, inputs):
    # Result = pd.read_csv("因子test1组批量测试-排序结果.csv",index_col=0)
    # Result_feature = list(Result["新增因子"])
    factor_last = []  # 做一个空列表储存已经测试过的因子
    factor_pool = ['sum(low_0/close_0,10)/sum (low_0/close_0,20)',
                   'sum(high_0/close_0,20)/sum (close_0/low_0,10)',
                   'correlation (turn_0, return_0,5)',
                   'rank (mean (amount_0/deal_number_0,10))/rank (mean (amount_o/deal_number_0,5))',
                   'sum(high_0/close_0,5)/sum(high_0/close_0,20)',
                   'ta_wma_20_0/ta_wma_5_0',
                   'correlation (volume_0, return_0,5)',
                   'alpha21=-1*sign (ta_stoch_slowk_5_3_0_3_0_0-ta_stoch_slowd_5_3_0_3_0_0)/price_limit_status_6',
                   'alpha22=close_0/turn_1-close_1/turn_3-close_2/turn_5',
                   'alpha23=daily_return_3/rank_avg_amount_3',
                   'alpha24 = close_0 * avg_turn_0 + close_1 * avg_turn_1 + close_2 * avg_turn_2',
                   'alpha25=(sum((close_0-open_0)/open_0>0.03,5)+sum((close_0-open_0)/open_0>0.03, 10)+sum ((close_0-open_0)/open_0>0.03,60))/std (sum((close_0-open_0)/open_0',
                   'alpha26 - std(amount_0, 6)']

    batch_num = 2  # 多少20组,需要跑多少组策略100
    batch_factor = list()
    for i in range(batch_num):
        random.seed(i)
        factor_num = 1  # 每组多少个因子
        batch_factor.append(random.sample(factor_pool, factor_num))
        parameters_list = []
        for feature in batch_factor:
            if feature in factor_last:
                print("continue")
                continue
            print(feature)
            factor_last.append(feature)
            # Result['因子数'] = len(feature)  # 这里计数总共有测试了多少个因子
            # Result['新增因子'] = [feature]  # 这里记录新测试的是哪个因子
            # Result.to_csv('因子表.csv', header=['新增因子', '因子数'], mode='a')  # 把测试好的因子追加写入因子表
            parameters = {'m25.features': '\n'.join(feature)}
            parameters_list.append({'parameters': parameters})

    def run(parameters):
        try:
            return g.run(parameters)
        except Exception as e:
            print('ERROR-----------', e)
            return None

    results = T.parallel_map(run, parameters_list, max_workers=2, remote_run=True, silent=True)  # 任务数 # 是否远程#
