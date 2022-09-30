def bigquant_run():
    param_grid = {}

    # 在这里设置需要调优的参数备选
    param_grid['m4.number_of_trees'] = [5, 10, 20]
    param_grid['m19.volume_limit'] = [0.025, 0.03]

    return param_grid

def bigquant_run():
    param_grid = {}

    # 在这里设置需要调优的参数备选
    param_grid['m3.features'] = ['close_1/close_0', 'close_2/close_0\nclose_3/close_0']
    param_grid['m4.number_of_trees'] = [5, 10, 20]

    return param_grid


def bigquant_run():
    import itertools
    param_grid = {}

    cond1 = [0.1, 0.3, 0.5, 0.7, 0.9]
    cond2 = [-3, -1, 0, 1, 3]
    list = []
    for a1 in cond1:
        for a2 in cond2:
            cond = 'cond1<{a_1} & cond2<{a_2}'.format(a_1=a1, a_2=a2)
            list.append(cond)

    param_grid['m9.expr'] = list

    return param_grid

def bigquant_run():
    import itertools
    param_grid = {}

    cond1 = [0.01,0.02,0.03,0.04,0.05,0.06,0.07,0.08,0.09]
    list = []
    for a1 in cond1:
        cond = 'cond1>{a_1}'.format(a_1=a1)
        list.append(cond)

    param_grid['m9.expr'] = list

    return param_grid