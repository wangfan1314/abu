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

    # 1. 资金分配
    # 平均持仓时间是hold_days，每日都将买入股票，每日预期使用 1/hold_days 的资金
    # 实际操作中，会存在一定的买入误差，所以在前hold_days天，等量使用资金；之后，尽量使用剩余资金（这里设置最多用等量的1.5倍）
    is_staging = context.trading_day_index < context.options['hold_days']  # 是否在建仓期间（前 hold_days 天）
    cash_avg = context.portfolio.portfolio_value / context.options['hold_days']
    cash_for_buy = min(context.portfolio.cash, (1 if is_staging else 1.5) * cash_avg)
    cash_for_sell = cash_avg - (context.portfolio.cash - cash_for_buy)
    positions = {e.symbol: p.amount * p.last_sale_price
                 for e, p in context.portfolio.positions.items()}

    # ------------------------START:止赢止损模块(含建仓期)---------------
    stock_sold = []  # 记录卖出的股票，防止多次卖出出现空单
    current_stoploss_stock = []
    current_stophold_stock = []
    # print(context.portfolio.positions.items())
    if len(positions) > 0:
        # print(today, '今日持仓列表:', positions.keys())
        for instrument in positions.keys():
            # 计算持股天数
            if instrument in context.instrument_hold_days:
                context.instrument_hold_days[instrument] = context.instrument_hold_days[instrument] + 1
            else:
                context.instrument_hold_days[instrument] = 1

            # 持股满hold_days天且当日非涨停则卖出
            if instrument not in stock_sold and context.instrument_hold_days[instrument] >= context.options['hold_days']-1 and data.can_trade(context.symbol(instrument)):
                context.order_target_percent(context.symbol(instrument), 0)
                cash_for_sell -= positions[instrument]
                current_stoploss_stock.append(instrument)
                context.instrument_hold_days.pop(instrument)
                stock_sold.append(instrument)

        if len(current_stoploss_stock) > 0:
            # print(today, '止损股票列表', current_stoploss_stock)
            stock_sold += current_stoploss_stock

    # --------------------------END: 止赢止损模块--------------------------

    # 计算今日跌停的股票
    dt_list = list(ranker_prediction[ranker_prediction.price_limit_status_0 == 1].instrument)
    zt_list = list(ranker_prediction[ranker_prediction.price_limit_status_0 == 1].instrument)
    banned_list = dt_list + zt_list

    hold_count = len(positions)
    max_count = context.options['hold_days'] * context.stock_count
    today_buy_count = min(max_count - hold_count, context.stock_count)
    today_buy_count = max_count - hold_count
    buy_cash_weights = T.norm([1 / math.log(i + 2) for i in range(0, today_buy_count)])
    buy_instruments = [k for k in list(ranker_prediction.instrument) if k not in banned_list][:len(buy_cash_weights)]
    max_cash_per_instrument = context.portfolio.portfolio_value * context.max_cash_per_instrument

    for i, instrument in enumerate(buy_instruments):
        cash = cash_for_buy * buy_cash_weights[i]
        if cash > max_cash_per_instrument - positions.get(instrument, 0):
            # 确保股票持仓量不会超过每次股票最大的占用资金量
            cash = max_cash_per_instrument - positions.get(instrument, 0)
        if cash > 0 and cash > 2000:
            order_id = context.order_value(context.symbol(instrument), cash)
            order_object = context.get_order(order_id)
            if order_object:
                buy_close = data.current(context.symbol(instrument), 'close')
                buy_actual = buy_close * order_object.amount
                # cash_for_buy -= buy_actual
            else:
                print('today', today, 'instrument', instrument, '订单为空')


