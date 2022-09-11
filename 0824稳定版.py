# 回测引擎：每日数据处理函数，每天执行一次
def bigquant_run(context, data):
    # 按日期过滤得到今日的预测数据
    ranker_prediction = context.ranker_prediction[
        context.ranker_prediction.date == data.current_dt.strftime('%Y-%m-%d')]
    today = data.current_dt.strftime('%Y-%m-%d')
    positions = {e.symbol: p.amount * p.last_sale_price
                 for e, p in context.perf_tracker.position_tracker.positions.items()}

    context.fall_back_count = context.fall_back_count - 1
    zt_list = list(ranker_prediction[ranker_prediction.price_limit_status_0 == 3].instrument)
    try:
        # 大盘风控模块，读取风控数据
        bm_2 = ranker_prediction['bm_2'].values[0]
        bm_3 = ranker_prediction['bm_3'].values[0]
        bm_4 = ranker_prediction['bm_4'].values[0]
        if (bm_2 > 0 and bm_3 > 0 and bm_4 > 0):
            print(today, '大盘风控止损触发,全仓卖出')
            for instrument in positions.keys():
                context.order_target(context.symbol(instrument), 0)
            if context.fall_back_count <= 0:
                context.fall_back_count = 1
            else:
                context.fall_back_count = context.fall_back_count + 1
            return
    except:
        print(today, '缺失风控数据！')

    # 1. 资金分配
    # 平均持仓时间是hold_days，每日都将买入股票，每日预期使用 1/hold_days 的资金
    # 实际操作中，会存在一定的买入误差，所以在前hold_days天，等量使用资金；之后，尽量使用剩余资金（这里设置最多用等量的1.5倍）
    cash_avg = context.portfolio.portfolio_value / context.options['hold_days']
    cash_for_buy = min(context.portfolio.cash, cash_avg)
    cash_for_sell = cash_avg - (context.portfolio.cash - cash_for_buy)

    if context.fall_back_count > 0:
        print(today, "fallback不买入", context.fall_back_count)
        return
    else:
        # 3. 生成买入订单：按机器学习算法预测的排序，买入前面的stock_count只股票
        buy(context, data, ranker_prediction, positions, cash_for_buy, today)



    # ------------------------START:止赢止损模块(含建仓期)---------------
    stock_sold = []  # 记录卖出的股票，防止多次卖出出现空单
    current_stoploss_stock = []
    positions_cost = {e.symbol: p.cost_basis for e, p in context.portfolio.positions.items()}
    holds = len(positions);
    if holds > 0:
        one_stop = False
        for instrument in positions.keys():
            stock_cost = positions_cost[instrument]
            # 当前价格
            stock_market_price = data.current(context.symbol(instrument), 'price')
            high = data.current(context.symbol(instrument), 'high')
            close = data.current(context.symbol(instrument), 'close')
            price_history = data.history(context.symbol(instrument), fields="price", bar_count=2, frequency="1d")
            return0 = (price_history[-1] - price_history[-2]) / price_history[-2]
            # 计算移动最高价
            if instrument in context.instrument_high_price:
                if context.instrument_high_price[instrument] < high:
                    context.instrument_high_price[instrument] = high
            else:
                if stock_cost > high:
                    context.instrument_high_price[instrument] = stock_cost
                else:
                    context.instrument_high_price[instrument] = high
            # 计算持股天数
            if instrument in context.instrument_hold_days:
                context.instrument_hold_days[instrument] = context.instrument_hold_days[instrument] + 1
            else:
                context.instrument_hold_days[instrument] = 1

            # volume_since_buy = data.history(context.symbol(instrument), 'volume', 6, '1d')
            rate = stock_market_price / context.instrument_high_price[instrument] - 1
            # 亏5%并且为可交易状态就止损
            if rate <= -0.057 and return0 < 1.03 and data.can_trade(context.symbol(instrument)) and instrument not in stock_sold:
                context.order_target_percent(context.symbol(instrument), 0)
                cash_for_sell -= positions[instrument]
                cash_for_buy += positions[instrument]
                # print(today, instrument, '当前移动最高价', context.instrument_high_price[instrument], '当前价格', stock_market_price, '移动止损卖出', rate, '今日', return0)
                current_stoploss_stock.append(instrument)
                context.instrument_high_price.pop(instrument)
                context.instrument_hold_days.pop(instrument)
                stock_sold.append(instrument)
                one_stop = True
                holds = holds - 1

            # 持股满7天且当日非涨停则卖出
            if instrument not in stock_sold and context.instrument_hold_days[
                instrument] >= 5 and stock_market_price / stock_cost - 1 < 0.1 and close != high and data.can_trade(
                context.symbol(instrument)):
                context.order_target_percent(context.symbol(instrument), 0)
                cash_for_sell -= positions[instrument]
                cash_for_buy += positions[instrument]
                # print(today, instrument, '持股满7天卖出', 'stock_market_price', stock_market_price, 'stock_cost', stock_cost, '天数', context.instrument_hold_days[instrument])
                current_stoploss_stock.append(instrument)
                context.instrument_high_price.pop(instrument)
                context.instrument_hold_days.pop(instrument)
                stock_sold.append(instrument)
                holds = holds - 1

        if one_stop:
            context.continue_loss_day = context.continue_loss_day + 1
        else:
            context.continue_loss_day = 0
        if holds == 0:
            context.continue_loss_day = 100
        if context.continue_loss_day >= 2:
            print(today, "触发止损风控，fallback开始")
            context.fall_back_count = 4
        if len(current_stoploss_stock) > 0:
            # print(today, '止损股票列表', current_stoploss_stock)
            stock_sold += current_stoploss_stock


    # --------------------------END: 止赢止损模块--------------------------


    # 2. 生成卖出订单：hold_days天之后才开始卖出；对持仓的股票，按机器学习算法预测的排序末位淘汰
    # sell_active(context, ranker_prediction, positions, cash_for_sell, stock_sold, zt_list, cash_for_buy, today)


def buy(context, data, ranker_prediction, positions, cash_for_buy, today):
    # 计算今日跌停的股票
    dt_list = list(ranker_prediction[ranker_prediction.price_limit_status_0 == 1].instrument)
    zt_list = list(ranker_prediction[ranker_prediction.price_limit_status_0 == 1].instrument)
    banned_list = dt_list + zt_list
    buy_cash_weights = context.stock_weights
    buy_instruments = [k for k in list(ranker_prediction.instrument) if k not in banned_list][:len(buy_cash_weights)]
    max_cash_per_instrument = context.portfolio.portfolio_value * context.max_cash_per_instrument

    hold_count = len(positions)
    for i, instrument in enumerate(buy_instruments):
        cash = cash_for_buy * buy_cash_weights[i]
        if hold_count >= 2:
            break
        if hold_count == 1:
            cash = cash_for_buy
        if cash > max_cash_per_instrument - positions.get(instrument, 0):
            # 确保股票持仓量不会超过每次股票最大的占用资金量
            cash = max_cash_per_instrument - positions.get(instrument, 0)
        if cash > 0 and cash > 2000:
            price_history = data.history(context.symbol(instrument), fields="price", bar_count=2, frequency="1d")
            open_history = data.history(context.symbol(instrument), fields="open", bar_count=2, frequency="1d")
            high_history = data.history(context.symbol(instrument), fields="high", bar_count=2, frequency="1d")
            return0 = (price_history[-1] - price_history[-2]) / price_history[-2]
            zt_price = round((price_history[-2] * 1.1), 2)
            # if high_history[-1] == zt_price and price_history[-1] < zt_price and return0 < 0.06:
            if high_history[-1] == zt_price and price_history[-1] < zt_price and return0 < 0.06 and open_history[-1] < \
                    price_history[-1]:
                continue
            order_id = context.order_value(context.symbol(instrument), cash)
            order_object = context.get_order(order_id)
            if order_object:
                buy_close = data.current(context.symbol(instrument), 'close')
                buy_actual = buy_close * order_object.amount
                cash_for_buy -= buy_actual
            else:
                print('today', today, 'instrument', instrument, '订单为空')
        hold_count = hold_count + 1