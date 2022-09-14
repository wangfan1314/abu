# 回测引擎：每日数据处理函数，每天执行一次
def bigquant_run(context, data):
    positions = {e.symbol: p.amount * p.last_sale_price
                 for e, p in context.perf_tracker.position_tracker.positions.items()}
    today = data.current_dt.strftime('%Y-%m-%d')
    # 按日期过滤得到今日的预测数据
    ranker_prediction = context.ranker_prediction[
        context.ranker_prediction.date == today]

    try:
        # 大盘风控模块，读取风控数据
        bm_0 = ranker_prediction['bm_0'].values[0]
        if (bm_0 > 0):
            # print(today, '大盘风控止损触发,全仓卖出')
            for instrument in positions.keys():
                context.order_target(context.symbol(instrument), 0)
                return
    except:
        print(today, '缺失风控数据！')

    # 定义资金
    cash_for_buy = context.portfolio.cash
    # 获取今天预测的股票池
    buy_instruments = list(ranker_prediction.instrument)
    # 找到我们当前的股票持仓
    current_hold_stock = [equity.symbol for equity in context.portfolio.positions]
    # 定义 一个 列表 用来储存我们今天要卖出的股票
    sell_instruments = [instrument.symbol for instrument in context.portfolio.positions.keys()]

    # ———逻辑上先卖后买，防止资金不足———产生空单
    # 今天需要卖出的股票 存在于我们 当前的股票持仓中 这里 因为我们只有1只股票 所以可以直接卖掉
    today_to_sell = [i for i in sell_instruments[:5]]
    for instrument in today_to_sell:
        context.order_target(context.symbol(instrument), 0)
    # 今天需要买入的股票 存在于我们 模型当天预测的股票池buy_instruments中
    # 这里我们只买排名最靠前的第一名
    today_to_buy = [i for i in buy_instruments[:1]]

    # 使用一个for循环将预测的股票前N名买入
    # 为了方便统计，我们直接用所有的钱下单，all in 当天买入的股票
    for instrument in today_to_buy:
        context.order_value(context.symbol(instrument), cash_for_buy)

