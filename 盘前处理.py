def bigquant_run(context, data):
    # 获取涨跌停状态—主力资金流入数据
    df_stock_status = context.status_df.set_index('date')
    today = data.current_dt.strftime('%Y-%m-%d');
    # 得到当前未完成订单
    for orders in get_open_orders().values():
        for _order in orders:
            ins = str(_order.sid.symbol)
            try:
                # 判断一下如果当日涨停,则取消卖单
                price_status = df_stock_status[df_stock_status.instrument == ins].price_limit_status_0.loc[today]
                if price_status > 2 and _order.amount < 0:
                    cancel_order(_order)
                    print(today, '尾盘涨停取消卖单', ins)
            # 判断一下如果当日收盘主力资全流入,则取消卖单
            # if df_stock_status[df_stock_status.instrument==ins].mf_net_amount_main_0.ix[today]>0 and _order.amount<0:
            # 	cancel_order(_order)
            # 	print(today,'当日收盘主力资金流入取消卖单',ins)
            except:
                print('error')
                continue


def bigquant_run(context):
	#获取st状态和涨跌停状态，超大单流入，主力资金流入状态
	context.status_df = D.features(instruments = context.instruments, start_date = context.start_date, end_date = context.end_date,
		fields=['price_limit_status_0'])