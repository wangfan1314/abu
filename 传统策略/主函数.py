# 回测引擎：每日数据处理函数，每天执行一次
def bigquant_run(context, data):
    # 获取今日的日期
    today = data.current_dt.strftime('%Y-%m-%d')
    # 通过positions对象，使用列表生成式的方法获取目前持仓的股票列表
    stock_hold_now = {e.symbol: p.amount * p.last_sale_price
                      for e, p in context.perf_tracker.position_tracker.positions.items()}

    # 记录用于买入股票的可用现金,因为是早盘卖股票，需要记录卖出的股票市值并在买入下单前更新可用现金；
    # 如果是早盘买尾盘卖，则卖出时不需更新可用现金，因为尾盘卖出股票所得现金无法使用
    cash_for_buy = context.portfolio.cash

    try:
        buy_stock = context.daily_stock_buy[today]  # 当日符合买入条件的股票
    except:
        buy_stock = []  # 如果没有符合条件的股票，就设置为空

    try:
        sell_stock = context.daily_stock_sell[today]  # 当日符合卖出条件的股票
    except:
        sell_stock = []  # 如果没有符合条件的股票，就设置为空

    # 需要卖出的股票:已有持仓中符合卖出条件的股票
    stock_to_sell = [i for i in stock_hold_now if i in sell_stock]
    # 需要买入的股票:没有持仓且符合买入条件的股票
    stock_to_buy = [i for i in buy_stock if i not in stock_hold_now]
    # 需要调仓的股票：已有持仓且不符合卖出条件的股票
    stock_to_adjust = [i for i in stock_hold_now if i not in sell_stock]

    # 如果有卖出信号
    if len(stock_to_sell) > 0:
        for instrument in stock_to_sell:
            sid = context.symbol(instrument)  # 将标的转化为equity格式
            cur_position = context.portfolio.positions[sid].amount  # 持仓
            if cur_position > 0 and data.can_trade(sid):
                context.order_target_percent(sid, 0)  # 全部卖出
                # 因为设置的是早盘卖出早盘买入，需要根据卖出的股票更新可用现金；如果设置尾盘卖出早盘买入，则不需更新可用现金(可以删除下面的语句)
                cash_for_buy += stock_hold_now[instrument]

    # 如果有买入信号/有持仓
    if len(stock_to_buy) + len(stock_to_adjust) > 0:
        weight = 1 / (len(stock_to_buy) + len(stock_to_adjust))  # 每只股票的比重为等资金比例持有
        for instrument in stock_to_buy + stock_to_adjust:
            sid = context.symbol(instrument)  # 将标的转化为equity格式
            if data.can_trade(sid):
                context.order_target_value(sid, weight * cash_for_buy)  # 买入