def sell_active(context, ranker_prediction, positions, cash_for_sell, stock_sold, zt_list, cash_for_buy, today):
    if cash_for_sell > 0:
        equities = {e.symbol: e for e, p in context.perf_tracker.position_tracker.positions.items()}
        # equities = {e.symbol: e for e, p in context.portfolio.positions.items()}
        instruments = list(reversed(list(ranker_prediction.instrument[ranker_prediction.instrument.apply(
            lambda x: x in equities and not context.has_unfinished_sell_order(equities[x]))])))
        # print('rank order for sell %s' % instruments)
        for instrument in instruments:
            # 如果资金够了就不卖出了
            if cash_for_sell <= 0:
                print(today, instrument, cash_for_sell)
                break
            # 防止多个止损条件同时满足，出现多次卖出产生空单
            if instrument in stock_sold:
                continue
            # 涨停不卖出
            if instrument in zt_list:
                # print(today, instrument, "涨停继续持有")
                continue
            print(today, '主动卖出', instrument, '持股天数',context.trading_day_index)
            context.order_target(context.symbol(instrument), 0)
            cash_for_sell -= positions[instrument]
            cash_for_buy += positions[instrument]
            stock_sold.append(instrument)
            context.instrument_high_price.pop(instrument)