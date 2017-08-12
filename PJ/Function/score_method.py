# -*- coding:utf-8 -*-
# Copyright GDP Group
# Created by songzhichen and ruchuang gao on July 8 2017
import pandas as pd
import numpy as np


# 输入所有调仓日因子表，所有调仓日因子权重表，调仓日期序列，返回所有调仓日的持仓表
def scorepercent(CPD_factor, weight, n, condition="top"):
    # 取得参数的deepcopy 防止污染参数数据
    CPD_factor = CPD_factor.copy()
    weight = weight.copy()

    factor_weight = pd.merge(CPD_factor, weight, on='time')
    columns_factor = list(filter(lambda x: x[-1] == 'x', factor_weight.columns))
    columns_weight = list(filter(lambda x: x[-1] == 'y', factor_weight.columns))
    factor_weight['score'] = 0
    for x, y in zip(columns_factor, columns_weight):
        factor_weight.loc[:, 'score'] = factor_weight.loc[:, x] * factor_weight.loc[:, y]
    score = factor_weight[['time', 'stkcd', 'score']].copy().dropna()

    # 定义一个函数，输入一个dataframe,返回score列排名前n的一个dataframe
    def get_N(df):
        # return df[df["score"].rank(ascending=(condition != "top")) <= n][['time', 'stkcd']]
        df = df.copy()
        df_sort = df.sort_values("score", ascending=True)
        if condition == "top":
            return df_sort.tail(n)
        else:
            return df_sort.head(n)

    return score.groupby("time").apply(get_N).reset_index(drop=True)


# 输入所有调仓日因子表，所有调仓日因子权重表，调仓日期序列，返回所有调仓日的持仓表
def scorepercent_industry(CPD_factor, weight, CPD_inudstry_weight, CPD_stk_inudstry, n, condition="top"):
    # 取得参数的deepcopy 防止污染参数数据
    CPD_factor = CPD_factor.copy()
    weight = weight.copy()
    CPD_inudstry_weight = CPD_inudstry_weight.copy()
    CPD_stk_inudstry = CPD_stk_inudstry.copy()

    # 得到调仓日的stk打分表
    factor_weight = pd.merge(CPD_factor, weight, on='time')
    columns_factor = list(filter(lambda x: x[-1] == 'x', factor_weight.columns))
    columns_weight = list(filter(lambda x: x[-1] == 'y', factor_weight.columns))
    factor_weight['score'] = 0
    for x, y in zip(columns_factor, columns_weight):
        factor_weight.loc[:, 'score'] = factor_weight.loc[:, x] * factor_weight.loc[:, y]
    score = factor_weight[['time', 'stkcd', 'score']].copy().dropna()

    # 计算调仓日股票的分配
    # 保证选满n只，同时如果行业内数量不够多，挪至其他行业
    def allocate_n(df):
        # copy data
        df = df.copy()
        basic_num = n // len(df)
        residul_num = n % len(df)
        df_sort = df.sort_values("weight", ascending=True)
        # df_basicplus和df_basic构建
        df_basicplus = df_sort.tail(residul_num)
        df_basicplus.loc[:, "allocate_num"] = basic_num + 1
        df_basic = df_sort.head(len(df_sort) - residul_num)
        df_basic.loc[:, "allocate_num"] = basic_num
        result = df_basicplus.append(df_basic)
        return result

    CPD_inudstry_weight = CPD_inudstry_weight.groupby("time").apply(allocate_n).reset_index(drop=True)

    # 进行三表merge
    finaltable_temp = pd.merge(score, CPD_stk_inudstry, on=["time", "stkcd"])
    finaltable = pd.merge(finaltable_temp, CPD_inudstry_weight, on=["time", "industry"]).dropna()

    # 根据time和industry groupby 计算获得仓位表
    def get_CPD_position(df):
        # copy data
        df = df.copy()
        number = int(df.allocate_num.iloc[0])
        # 默认行业内部等权分配，此问题有时间再考虑是否加入其他权重方法
        df.loc[:, "weight"] = df.loc[:, "weight"] / number
        df_sort = df.sort_values("score", ascending=True)
        # 取出对应个数的股票
        if condition == "top":
            return df_sort.tail(number).drop(["score", "industry", "allocate_num"], axis=1)
        else:
            return df_sort.head(number).drop(["score", "industry", "allocate_num"], axis=1)

    position_temp = finaltable.groupby(["time", "industry"]).apply(get_CPD_position).reset_index(drop=True)

    # 因为数据库的行业占比加总可能并非100（由于约分），特此单独处理一次
    def weight_hundred(df):
        df = df.copy()
        df.weight = df.weight / np.sum(df.weight)
        return df

    CPD_position = position_temp.groupby("time").apply(weight_hundred).reset_index(drop=True)
    return CPD_position
