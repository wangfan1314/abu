def bigquant_run(bq_graph, inputs):
    features = ['shift(close, -5) / shift(open, -1)', 'shift(close, -20) / shift(open, -1)']

    parameters_list = []

    for feature in features:
        label_ = feature + """
            \n all_wbins(label, 20)
            \n clip(label, all_quantile(label, 0.01), all_quantile(label, 0.99))
            \n where(shift(high, -1) == shift(low, -1), NaN, label)

        """

        parameters = {'m2.label_expr': feature}
        parameters_list.append({'parameters': parameters})

    def run(parameters):
        try:
            print(parameters)
            return g.run(parameters)
        except Exception as e:
            print('ERROR --------', e)
            return None

    results = T.parallel_map(run, parameters_list, max_workers=2, remote_run=False, silent=True)

    return results, parameters_list
