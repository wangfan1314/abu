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

    batch_num = 5  # 多少20组,需要跑多少组策略100
    batch_factor = list()
    for i in range(batch_num):
        random.seed(i)
        factor_num = 1  # 每组多少个因子
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