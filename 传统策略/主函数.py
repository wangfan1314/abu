# 回测引擎：每日数据处理函数，每天执行一次
def bigquant_run(context, data):
    # 获取今日的日期
    today = data.current_dt.strftime('%Y-%m-%d')
    # 通过positions对象，使用列表生成式的方法获取目前持仓的股票列表
    stock_hold_now = {e.symbol: p.amount * p.last_sale_price
                      for e, p in context.perf_tracker.position_tracker.positions.items()}

    # 1. 资金分配
    # 平均持仓时间是hold_days，每日都将买入股票，每日预期使用 1/hold_days 的资金
    # 实际操作中，会存在一定的买入误差，所以在前hold_days天，等量使用资金；之后，尽量使用剩余资金（这里设置最多用等量的1.5倍）
    is_staging = context.trading_day_index < context.options['hold_days']  # 是否在建仓期间（前 hold_days 天）
    cash_avg = context.portfolio.portfolio_value / context.options['hold_days']
    cash_for_buy = min(context.portfolio.cash, (1 if is_staging else 1) * cash_avg)
    # print(today, ":", cash_for_buy)

    try:
        buy_stock = context.daily_stock_buy[today]  # 当日符合买入条件的股票
    except:
        buy_stock = []  # 如果没有符合条件的股票，就设置为空

    # 需要买入的股票:没有持仓且符合买入条件的股票
    stock_to_buy = [i for i in buy_stock if i not in stock_hold_now][:context.stock_count]

    # 如果有买入信号/有持仓
    if len(stock_to_buy) > 0:
        weight = 1 / (len(stock_to_buy))  # 每只股票的比重为等资金比例持有
        for instrument in stock_to_buy:
            sid = context.symbol(instrument)  # 将标的转化为equity格式
            if data.can_trade(sid):
                context.order_target_value(sid, weight * cash_for_buy)  # 买入

    holds = len(stock_hold_now)
    stock_sold = []  # 记录卖出的股票，防止多次卖出出现空单
    current_stoploss_stock = []
    if holds > 0:
        for instrument in stock_hold_now.keys():
            # 计算持股天数
            if instrument in context.instrument_hold_days:
                context.instrument_hold_days[instrument] = context.instrument_hold_days[instrument] + 1
            else:
                context.instrument_hold_days[instrument] = 1

            # print(today, instrument, 'hold:',context.instrument_hold_days[instrument])
            # 持股满7天且当日非涨停则卖出
            if instrument not in stock_sold and context.instrument_hold_days[instrument] >= context.options['hold_days']-1 and data.can_trade(context.symbol(instrument)):
                context.order_target_percent(context.symbol(instrument), 0)
                current_stoploss_stock.append(instrument)
                context.instrument_hold_days.pop(instrument)
                stock_sold.append(instrument)

        if len(current_stoploss_stock) > 0:
            # print(today, '止损股票列表', current_stoploss_stock)
            stock_sold += current_stoploss_stock

