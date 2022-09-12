# --------------------------------------------------------------------
# 卖出条件
# --------------------------------------------------------------------
def sell_action(context, data):
    date = data.current_dt.strftime('%Y-%m-%d')
    hit_stop_stock = context.stock_hit_stop

    try:
        today_enter_stock = context.enter_daily_df.loc[date]
    except KeyError as e:
        today_enter_stock = []
    try:
        today_exit_stock = context.exit_daily_df.loc[date]
    except KeyError as e:
        today_exit_stock = []

    target_stock_to_buy = [i for i in context.selected_stock if i in today_enter_stock]
    stock_hold_now = [equity.symbol for equity in context.portfolio.positions]  # 当前持仓股票

    if context.trading_day_index % context.sell_frequency == 0:
        stock_to_sell = [i for i in stock_hold_now if i in today_exit_stock]  # 要卖出的股票
        stock_buy_and_sell = [i for i in stock_to_sell if i in target_stock_to_buy]
        if context.is_sell_willbuy_stock == False:  # 要买入的股票不卖出,但该票也不再买入
            stock_to_sell.extend(hit_stop_stock)  # 将触发个股风控的股票融入到卖出票池
            stock_to_sell = [i for i in stock_to_sell if i not in stock_buy_and_sell]  # 进行更新而已
        elif context.is_sell_willbuy_stock == True:  # 要买入的股票依然要卖出,该票不再买入
            stock_to_sell.extend(hit_stop_stock)

        # 买入时需要过滤的股票
        context.cannot_buy_stock = stock_buy_and_sell

        for stock in stock_to_sell:
            if data.can_trade(context.symbol(stock)):
                context.order_target_percent(context.symbol(stock), 0)
                del context.portfolio.positions[context.symbol(stock)]


# --------------------------------------------------------------------
# 买入条件
# --------------------------------------------------------------------
def buy_action(context, data):
    date = data.current_dt.strftime('%Y-%m-%d')

    try:
        today_enter_stock = context.enter_daily_df.loc[date]
    except KeyError as e:
        today_enter_stock = []
    try:
        today_exit_stock = context.exit_daily_df.loc[date]
    except KeyError as e:
        today_exit_stock = []

    target_stock_to_buy = [i for i in context.selected_stock if i in today_enter_stock]
    target_stock_to_buy = [s for s in target_stock_to_buy if s not in context.cannot_buy_stock]  # 进行更新，不能买入的股票要过滤

    stock_hold_now = [equity.symbol for equity in context.portfolio.positions]  # 当前持仓股票

    # 确定股票权重
    if context.order_weight_method == 'equal_weight':
        equal_weight = 1 / context.max_stock_count

    portfolio_value = context.portfolio.portfolio_value
    position_current_value = {pos.sid: pos.amount * pos.last_sale_price for i, pos in
                              context.portfolio.positions.items()}

    # 买入
    if context.trading_day_index % context.buy_frequency == 0:
        if len(stock_hold_now) >= context.max_stock_count:
            return

        today_buy_count = 0
        if context.trade_mode == '轮动':
            for s in target_stock_to_buy:
                if today_buy_count + len(stock_hold_now) >= context.max_stock_count:  # 超出最大持仓数量
                    break
                if data.can_trade(context.symbol(s)):
                    order_target_percent(context.symbol(s), equal_weight)
                    today_buy_count += 1
        else:
            if context.can_duplication_buy == True:  # 可以重复买入，多一份买入
                for s in target_stock_to_buy:
                    if today_buy_count + len(stock_hold_now) >= context.max_stock_count:  # 超出最大持仓数量
                        break

                    if data.can_trade(context.symbol(s)):
                        if context.symbol(s) in position_current_weight:
                            curr_value = position_current_value.get(context.symbol(s))
                            order_value(context.symbol(s), min(context.max_stock_weight * portfolio_value - curr_value,
                                                               equal_weight * portfolio_value))
                        else:
                            order_value(context.symbol(s), equal_weight * portfolio_value)
                        today_buy_count += 1

            elif context.can_duplication_buy == False:  # 不可以重复买入，不买
                for s in target_stock_to_buy:
                    if today_buy_count + len(stock_hold_now) >= context.max_stock_count:  # 超出最大持仓数量
                        break
                    if s in stock_hold_now:
                        continue
                    else:
                        if data.can_trade(context.symbol(s)):
                            order_target_percent(context.symbol(s), equal_weight)
                            today_buy_count += 1


# --------------------------------------------------------------------
# 风控体系
# --------------------------------------------------------------------
def market_risk_manage(context, data):
    """大盘风控"""
    date = data.current_dt.strftime('%Y-%m-%d')
    if type(context.index_signal_data) == pd.DataFrame:
        current_signal = context.index_signal_data.loc[date]['signal']
        if current_signal == 'short':
            stock_hold_now = [equity.symbol for equity in context.portfolio.positions]
            # 平掉所有股票
            for stock in stock_hold_now:
                if data.can_trade(context.symbol(stock)):
                    context.order_target_percent(context.symbol(stock), 0)
            print('大盘出现止损信号， 平掉全部仓位，并关闭交易！')
            context.market_risk_signal = 'short'
    else:
        context.market_risk_signal = 'long'


def strategy_risk_manage(context, data):
    """策略风控"""
    if context.strategy_risk_conf == []:  # 没有设置策略风控
        context.strategy_risk_signal = 'long'

    else:
        for rm in context.strategy_risk_conf:
            if rm['method'] == 'strategy_percent_stopwin':
                pct = rm['params']['percent']
                portfolio_value = context.portfolio.portfolio_value
                if portfolio_value / context.capital_base - 1 > pct:
                    stock_hold_now = [equity.symbol for equity in context.portfolio.positions]
                    # 平掉所有股票
                    for stock in stock_hold_now:
                        if data.can_trade(context.symbol(stock)):
                            context.order_target_percent(context.symbol(stock), 0)
                    print('策略出现止盈信号， 平掉全部仓位，并关闭交易！')
                    context.strategy_risk_signal = 'short'

            if rm['method'] == 'strategy_percent_stoploss':
                pct = rm['params']['percent']
                portfolio_value = context.portfolio.portfolio_value
                if portfolio_value / context.capital_base - 1 < pct:
                    stock_hold_now = [equity.symbol for equity in context.portfolio.positions]
                    # 平掉所有股票
                    for stock in stock_hold_now:
                        if data.can_trade(context.symbol(stock)):
                            context.order_target_percent(context.symbol(stock), 0)
                    print('策略出现止损信号， 平掉全部仓位，并关闭交易！')
                    context.strategy_risk_signal = 'short'


def stock_risk_manage(context, data):
    """个股风控"""
    position_current_pnl = {pos.sid: (pos.last_sale_price - pos.cost_basis) / pos.cost_basis for i, pos in
                            context.portfolio.positions.items()}

    for rm in context.stock_risk_conf:
        params_pct = rm['params']['percent']
        if rm['method'] == 'stock_percent_stopwin':
            for sid, pnl_pct in position_current_pnl.items():
                if pnl_pct > params_pct:
                    context.stock_hit_stop.append(sid.symbol)

        if rm['method'] == 'stock_percent_stoploss':
            for sid, pnl_pct in position_current_pnl.items():
                if pnl_pct < params_pct:
                    context.stock_hit_stop.append(sid.symbol)


# 回测引擎：每日数据处理函数，每天执行一次
def bigquant_run(context, data):
    """每日运行策略逻辑"""
    market_risk_manage(context, data)
    strategy_risk_manage(context, data)

    if context.market_risk_signal == 'short': return
    if context.strategy_risk_signal == 'short': return

    stock_risk_manage(context, data)

    sell_action(context, data)
    buy_action(context, data)
