# 回测引擎：每日数据处理函数，每天执行一次
def bigquant_run(context, data):
    # 按日期过滤得到今日的预测数据
    positions = {e.symbol: p.amount * p.last_sale_price
                 for e, p in context.portfolio.positions.items()}
    today = data.current_dt.strftime('%Y-%m-%d')
    ranker_prediction = context.ranker_prediction[
        context.ranker_prediction.date == data.current_dt.strftime('%Y-%m-%d')]

    try:
        # 大盘风控模块，读取风控数据
        benckmark_risk = ranker_prediction['bm_0'].values[0]
        # if benckmark_risk > 0:
        # for instrument in positions.keys():
        # context.order_target(context.symbol(instrument), 0)
        # print(today,'大盘风控止损触发,全仓卖出')
        # return
    except:
        print('缺失风控数据！')

    # 当risk为1时，市场有风险，全部平仓，不再执行其它操作

    # 1. 资金分配
    # 平均持仓时间是hold_days，每日都将买入股票，每日预期使用 1/hold_days 的资金
    # 实际操作中，会存在一定的买入误差，所以在前hold_days天，等量使用资金；之后，尽量使用剩余资金（这里设置最多用等量的1.5倍）
    is_staging = context.trading_day_index < context.options['hold_days']  # 是否在建仓期间（前 hold_days 天）
    cash_avg = context.portfolio.portfolio_value / context.options['hold_days']
    cash_for_buy = min(context.portfolio.cash, (1 if is_staging else 1.5) * cash_avg)
    cash_for_sell = cash_avg - (context.portfolio.cash - cash_for_buy)

    # ------------------------START:止赢止损模块(含建仓期)---------------
    stock_sold = []  # 记录卖出的股票，防止多次卖出出现空单
    current_stopwin_stock = []
    current_stoploss_stock = []
    positions_cost = {e.symbol: p.cost_basis for e, p in context.portfolio.positions.items()}
    if len(positions) > 0:
        for instrument in positions.keys():
            stock_cost = positions_cost[instrument]
            stock_market_price = data.current(context.symbol(instrument), 'price')
            # volume_since_buy = data.history(context.symbol(instrument), 'volume', 6, '1d')
            # 赚9%且为可交易状态就止盈
            if stock_market_price / stock_cost - 1 >= 0.30 and data.can_trade(context.symbol(instrument)):
                print(today, '止盈', instrument, ",持仓:", positions[instrument])
                context.order_target_percent(context.symbol(instrument), 0)
                cash_for_sell -= positions[instrument]
                current_stopwin_stock.append(instrument)
            # 亏5%并且为可交易状态就止损
            if stock_market_price / stock_cost - 1 <= -0.5 and data.can_trade(context.symbol(instrument)):
                context.order_target_percent(context.symbol(instrument), 0)
                cash_for_sell -= positions[instrument]
                current_stoploss_stock.append(instrument)
            # 放天量  止损：
            # if (volume_since_buy[0]>1.5*volume_since_buy[1]) |(volume_since_buy[0]>1.5*(volume_since_buy[1]+volume_since_buy[2]+volume_since_buy[3]+volume_since_buy[4]+volume_since_buy[5])/5):
            #    context.order_target_percent(context.symbol(instrument),0)
            #    cash_for_sell -= positions[instrument]
            #    current_stoploss_stock.append(instrument)
        if len(current_stopwin_stock) > 0:
            print(today, '止盈股票列表', current_stopwin_stock)
            stock_sold += current_stopwin_stock
        if len(current_stoploss_stock) > 0:
            print(today, '止损股票列表', current_stoploss_stock)
            stock_sold += current_stoploss_stock

    # --------------------------END: 止赢止损模块--------------------------

    # 2. 生成卖出订单：hold_days天之后才开始卖出；对持仓的股票，按机器学习算法预测的排序末位淘汰
    current_stopdays_stock = []
    if not is_staging and cash_for_sell > 0:
        equities = {e.symbol: e for e, p in context.portfolio.positions.items()}
        instruments = list(reversed(list(ranker_prediction.instrument[ranker_prediction.instrument.apply(
            lambda x: x in equities)])))

        for instrument in instruments:
            # 如果资金够了就不卖出了
            if cash_for_sell <= 0:
                break
            # 防止多个止损条件同时满足，出现多次卖出产生空单
            if instrument in stock_sold:
                continue
            context.order_target(context.symbol(instrument), 0)
            cash_for_sell -= positions[instrument]
            stock_sold.append(instrument)

            # 防止多个止损条件同时满足，出现多次卖出产生空单
            # if instrument in stock_sold:
            #     continue
            # context.order_target(context.symbol(instrument), 0)
            # current_stopdays_stock.append(instrument)
            # cash_for_sell -= positions[instrument]
            # #if len(current_stopdays_stock)>0:
            #     #print(today,'固定天数卖出列表',current_stopdays_stock)
            # stock_sold += current_stopdays_stock
            # if cash_for_sell <= 0:
            #     break

    # 3. 生成买入订单：按机器学习算法预测的排序，买入前面的stock_count只股票
    buy_cash_weights = context.stock_weights
    buy_instruments = list(ranker_prediction.instrument[:len(buy_cash_weights)])
    max_cash_per_instrument = context.portfolio.portfolio_value * context.max_cash_per_instrument
    for i, instrument in enumerate(buy_instruments):
        cash = cash_for_buy * buy_cash_weights[i]
        if cash > max_cash_per_instrument - positions.get(instrument, 0):
            # 确保股票持仓量不会超过每次股票最大的占用资金量
            cash = max_cash_per_instrument - positions.get(instrument, 0)
        if cash > 0:
            context.order_value(context.symbol(instrument), cash)
