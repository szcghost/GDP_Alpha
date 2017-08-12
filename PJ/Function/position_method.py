# -*- coding:utf-8 -*-
# Copyright GDP Group
# Created by songzhichen and ruchuang gao on July 8 2017

import pandas as pd
import numpy as np


# 输入每日股票持仓序列，返回每日持仓表
def equal_weight(**kwargs):
    # 获取参数数据
    symbol = kwargs.get("symbol")
    # 构造返回持仓表
    final = pd.DataFrame()
    for date, group in symbol.groupby("time"):
        group["weight"] = 1.0 / group.shape[0]
        final = final.append(group)
    return final


# 过去20个交易日波动率加权平均
def vol_weight(**kwargs):
    # 获取数据
    symbol = kwargs.get("symbol")
    vol = kwargs.get("vol")
    # 构造返回持仓表
    final = pd.DataFrame()
    for date, group in symbol.groupby("time"):
        group = pd.merge(group, vol, how="left", on=['time', 'stkcd'])
        group["weight"] = group["volatility"] / np.sum(group["volatility"])
        group = group.drop("volatility", axis=1)
        final = final.append(group)
    return final
    # # 构造返回持仓表
    # vol_final = pd.DataFrame()
    # for name, group in symbol.groupby("time"):
    #     date_list=trading.time.drop_duplicates().sort_values(ascending=True)
    #     pre20 = list(date_list[date_list<name])[-20]
    #     trading_temp=trading.query("time <= '{0}' and time >= '{1}'".format(name, pre20))
    #     stocklist = list(group.stkcd)
    #     trading_temp = trading_temp[trading_temp.stkcd.isin(stocklist)]
    #     trading_temp.loc[:, 'ptc'] = np.log(trading_temp.closep) - np.log(trading_temp.preclosep)
    #     # 计算波动率
    #     volatility = pd.DataFrame()
    #     i = 0
    #     for name2, group2 in trading_temp.groupby("stkcd"):
    #         volatility.loc[i, "stkcd"] = name2
    #         volatility.loc[i, "volatility"] = np.std(group2.ptc, ddof=1)
    #         volatility.loc[i, 'time'] = name
    #         i =i+1
    #     vol_final=vol_final.append(volatility)
    # sym_vol=pd.merge(symbol, vol_final, on=['time', 'stkcd'], how='left')
    # #加权平均
    # final=pd.DataFrame()
    # for name3,group3 in sym_vol.groupby("time"):
    #     group3.loc[:,"weight"]=group3.volatility/np.sum(group3.volatility)
    #     final=final.append(group3)
    # return final.loc[:,["time","stkcd","weight"]]


# 市值加权
def ev_weight(**kwargs):
    # 获取参数数据
    symbol = kwargs.get("symbol")
    ev = kwargs.get("ev")
    # 构造返回持仓表
    final = pd.DataFrame()
    for date, group in symbol.groupby("time"):
        group = pd.merge(group, ev, how="left", on=['time', 'stkcd'])
        group["weight"] = group["marketvalue"] / np.sum(group["marketvalue"])
        group = group.drop("marketvalue", axis=1)
        final = final.append(group)
    return final


if __name__ == "__main__":
    a = pd.DataFrame(columns=["time", "stkcd"])
    a["time"] = [1, 1, 3, 4]
    a["stkcd"] = ["a", "b", "c", "d"]
    print(equal_weight(a))
