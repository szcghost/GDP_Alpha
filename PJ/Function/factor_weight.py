# -*- coding:utf-8 -*-
# Copyright GDP Group
# Created by songzhichen and ruchuang gao on July 8 2017
import pandas as pd
import numpy as np


# 输入因子表和行情表，返回因子权重序列
def equal_weight(all_factor_data, all_trading_data, change_position_day, backperiod, all_tradingday, benchmark):
    # 取得参数的deepcopy 防止污染参数数据
    all_factor_data=all_factor_data.copy()
    change_position_day=change_position_day.copy()

    factor_list = [all_factor_data.columns[0]] + list(all_factor_data.columns[2:])
    result = pd.DataFrame(columns=factor_list)
    result.loc[:, "time"] = change_position_day.time
    result.iloc[:, 1:] = 1 / (len(factor_list) - 1)
    return result


def pca(all_factor_data, all_trading_data, change_position_day, backperiod, all_tradingday, benchmark):
    # 取得参数的deepcopy 防止污染参数数据
    all_factor_data=all_factor_data.copy()
    change_position_day=change_position_day.copy()
    all_tradingday=all_tradingday.copy()

    factor_list = [all_factor_data.columns[0]] + list(all_factor_data.columns[2:])
    result = pd.DataFrame(columns=factor_list)
    all_tradingday = list(all_tradingday.time)
    i = 0
    for date in change_position_day.time:
        # 得到过去backperiod的因子数据
        test_date = all_tradingday[all_tradingday.index(date) - backperiod:all_tradingday.index(date)]
        date_temp = pd.DataFrame()
        date_temp.loc[:, "time"] = test_date
        factor = pd.merge(date_temp, all_factor_data, on="time", how="left")
        factor = factor.dropna()
        # 使用pca方法计算因子权重
        n = len(factor.columns) - 2
        X = factor.iloc[:, -n:]
        weightList = np.linalg.eig(X.corr())[0]
        weightList = weightList / sum(weightList)
        result.loc[i, "time"] = date
        result.loc[i, list(all_factor_data.columns[2:])] = weightList
        i = i + 1
    return result


def regression(all_factor_data, all_trading_data, change_position_day, backperiod, all_tradingday, benchmark):
    # 取得参数的deepcopy 防止污染参数数据
    all_factor_data=all_factor_data.copy()
    all_trading_data=all_trading_data.copy()
    change_position_day=change_position_day.copy()
    all_tradingday=all_tradingday.copy()

    all_tradingday = list(all_tradingday.time)
    result = pd.DataFrame(columns=list(all_factor_data.columns[2:]))
    all_trading_data.loc[:, 'ptc'] = np.log(all_trading_data.loc[:, 'closep']) - \
                                     np.log(all_trading_data.loc[:, 'preclosep'])
    i = 0
    for date in change_position_day.time:
        regression_period1 = all_tradingday[(all_tradingday.index(date) - backperiod // 2):all_tradingday.index(date)]
        regression_period2 = all_tradingday[(all_tradingday.index(date) - backperiod):all_tradingday.index(
            date) - backperiod // 2]

        # 计算回归的x和y
        # 股票的收益率
        regression_data_stock = all_trading_data.query("stkcd != '{benchmark}' and time in {period}".
                                                       format(benchmark=benchmark,
                                                              period='(\'' + '\',\''.join(
                                                                  regression_period1) + '\')'))
        regression_data_stock = regression_data_stock.loc[:, ['time', 'stkcd', 'ptc']]

        # 因子值
        regression_data_factor = all_factor_data[all_factor_data.time.isin(regression_period2)]

        # 取20日内的平均值
        def get_mean(df):
            df=df.copy()
            return df.mean()

        mean_stock = regression_data_stock.groupby("stkcd").apply(get_mean).reset_index().dropna()
        mean_factor = regression_data_factor.groupby("stkcd").apply(get_mean).reset_index().dropna()

        # 得到回归的数据
        regression_data = pd.merge(mean_stock, mean_factor, how='left', on='stkcd').dropna()

        # 对回归变量标准化处理
        for j in regression_data.columns[1:]:
            regression_data[j] = (regression_data[j] - regression_data[j].mean()) \
                                 / regression_data[j].std()

        import statsmodels.api as sm
        Y = regression_data.loc[:, 'ptc']
        X = regression_data.iloc[:, 2:].copy()
        X = sm.add_constant(X)
        coef = list(sm.OLS(Y, X).fit().params[1:])
        weight = coef / sum(coef)
        result.loc[i, "time"] = date
        result.loc[i, list(regression_data.columns[2:])] = weight
        i += 1
    return result
