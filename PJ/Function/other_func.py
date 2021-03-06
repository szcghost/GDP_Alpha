# -*- coding:utf-8 -*-
# Copyright GDP Group
# Created by songzhichen and ruchuang gao on July 8 2017

# other useful function

from datetime import datetime as dt
import pandas as pd


# 根据月份返回季度
def quater(month):
    if month in ["01", "02", "03"]:
        return 1
    elif month in ["04", "05", "06"]:
        return 2
    elif month in ["07", "08", "09"]:
        return 3
    elif month in ["10", "11", "12"]:
        return 4


# 得到数据库内所有的交易日序列
def get_all_tradedate(conn):
    strSQL = "select distinct time from PE"
    return pd.read_sql_query(strSQL, conn)


# 根据起始日期，频率，从数据库中获取进行建仓调仓的日期序列
def tradeday(start, end, freq, all_tradingday):
    # 取得参数的deepcopy 防止污染参数数据
    all_tradingday = all_tradingday.copy()

    all_tradingday = all_tradingday.query("time  >='{}' and time <='{}'".format(start, end))
    all_tradingday.loc[:, "year"] = [i[:4] for i in all_tradingday.loc[:, "time"]]
    all_tradingday.loc[:, "month"] = [i[5:7] for i in all_tradingday.loc[:, "time"]]
    all_tradingday.loc[:, "day"] = [i[8:] for i in all_tradingday.loc[:, "time"]]
    # 根据freq，输出调仓日
    if freq == "M":
        result = pd.DataFrame()
        for (name1, name2), group in all_tradingday.groupby(["year", "month"]):
            result = result.append(group.head(1))
        return result.drop(["day", "month", "year"], axis=1)
    elif freq == "Q":
        # 得到每年4个quanter的月份
        result = pd.DataFrame()
        all_tradingday.loc[:, "quarter"] = [quater(i) for i in all_tradingday.loc[:, "month"]]
        for (name1, name2), group in all_tradingday.groupby(["year", "quarter"]):
            result = result.append(group.head(1))
        return result.drop(["day", "month", "year", "quarter"], axis=1)
    else:
        return all_tradingday.drop(["day", "month", "year"], axis=1)


# 根据开始日期和回归时间，返回数据获取的开始时间
def back_date(start, back_period, all_tradingday):
    # 取得参数的deepcopy 防止污染参数数据
    all_tradingday = all_tradingday.copy()

    strSQL = "select distinct time from PE"
    daylist = list(all_tradingday.time)
    return daylist[daylist.index(start) - back_period]


def get_id():
    date = dt.today()
    id = str(date.year) + str(date.month) + str(date.day) + str(date.hour) + str(date.minute) + str(date.second)
    return id


def get_tradefactor(all_factor_data, change_position_day):
    # 取得参数的deepcopy 防止污染参数数据
    all_factor_data = all_factor_data.copy()
    change_position_day = change_position_day.copy()

    return pd.merge(change_position_day, all_factor_data, on="time", how="left")


def revise_start(start, all_tradingday):
    # 取得参数的deepcopy 防止污染参数数据
    all_tradingday = all_tradingday.copy()

    while True:
        if start in all_tradingday:
            return start
        else:
            start = str(pd.date_range(start=start, periods=2, freq='D')[-1])[:10]


def revise_end(end, all_tradingday):
    # 取得参数的deepcopy 防止污染参数数据
    all_tradingday = all_tradingday.copy()
    while True:
        if end in all_tradingday:
            return end
        else:
            end = str(pd.date_range(end=end, periods=2, freq='D')[0])[:10]


# 输入调仓日的position表和调仓日序列、全部交易日序列，返回全部交易日的持仓表
# resample错误
# def position_extension(position, all_tradedate, freq):
#     if freq == "D":
#         return position
#     else:
#         position.time = [dt.strptime(i, '%Y-%m-%d') for i in position.time]
#         position = position.set_index("time")
#         result = pd.DataFrame()
#         for name, group in position.groupby("stkcd"):
#             result = result.append(group.resample('D').ffill())
#         result = result.reset_index()
#         result.time = [dt.strftime(i, '%Y-%m-%d') for i in result.time]
#         return result[result.time.isin(all_tradedate.time)]

# def position_extension(CPD_position, all_trading_data, freq, trade_status):
#     # 取得参数的deepcopy 防止污染参数数据
#     CPD_position = CPD_position.copy()
#     all_trading_data = all_trading_data.copy()
#
#     if freq == 'D':
#         return CPD_position
#     if freq == 'M':
#         # CPD_position 的time格式是timestamp
#         CPD_position.loc[:, 'YYMM'] = [i[:4] + i[5:7] for i in CPD_position.time]
#         # all_trading_date 的time格式是字符串
#         all_trading_data.loc[:, 'YYMM'] = [i[:4] + i[5:7] for i in all_trading_data.time]
#         result = pd.merge(all_trading_data, CPD_position, how='inner',
#                           on=['stkcd', 'YYMM'], suffixes=['', '_huancang'])
#         return result[['time', 'stkcd', "weight"]]


def position_extension(CPD_position, all_trading_data,
                       all_tradingday, change_position_day, freq,
                       trade_status, trade_limit):
    # 取得参数的deepcopy 防止污染参数数据
    CPD_position = CPD_position.copy()
    all_trading_data = all_trading_data.copy()
    all_tradingday = all_tradingday.copy()
    change_position_day = change_position_day.copy()
    trade_status = trade_status.copy()
    trade_limit = trade_limit.copy()

    # 把停牌和涨跌停合并到一张表里
    status_limit = pd.merge(trade_status, trade_limit, on=['time', 'stkcd'])

    # 找到每个持仓周期的时间list
    change_position_day = list(change_position_day.time)
    all_tradingday = list(all_tradingday.time)
    position_period = []
    for i in range(0, len(change_position_day) - 1):
        starttime = change_position_day[i]
        endtime = change_position_day[i + 1]
        position_period.append(
            all_tradingday[all_tradingday.index(starttime):all_tradingday.index(endtime)])
    position_period.append(
        all_tradingday[all_tradingday.index(endtime): all_tradingday.index(all_trading_data.time.max()) + 1])

    # 循环前的准备变量
    yesterday = 0
    yesterday_position = pd.DataFrame(columns=['time', 'stkcd', 'score', 'weight'])
    final_result = pd.DataFrame()
    for (name, group), period in zip(CPD_position.groupby('time'), position_period):
        for today in period:
            today_position = pd.DataFrame(columns=['time', 'stkcd', 'weight'])  # 重置数据
            if today == name:
                # 换仓日，仓位保持与上个月一样
                today_position = yesterday_position.copy()
                today_position['time'] = today
                # tosell = today_position[-today_position.stkcd.isin(group.stkcd)][['stkcd', 'score', 'weight']]
                # toselllist = list(tosell.stkcd)
                # tobuy = group[-group.isin(today_position.stkcd)][['stkcd', 'score', 'weight']]
                # tobuylist = list(tobuy.stkcd)
            elif yesterday == name:
                # 换仓日第二天
                # 仅从上个月的position和这个月的position的差异得出的selling和buying
                selling = yesterday_position[-yesterday_position.stkcd.isin(group.stkcd)]
                buying = group[-group.stkcd.isin(yesterday_position.stkcd)]

                if len(buying) == 0:
                    # 若buying为空，直接填充到月底或季度底
                    today_position = group.copy()
                    for i in period[period.index(today):]:
                        today_position['time'] = i
                        final_result = final_result.append(today_position)
                        yesterday_position = today_position.copy()
                    break
                # 查找buying和selling的交易状态
                selling = pd.merge(selling, status_limit, on=['time', 'stkcd'])
                buying = pd.merge(buying, status_limit, on=['time', 'stkcd'])

                # 确定tosell，并计算tosell的总权重
                tosell = selling[(selling.status != 'Trading') | (selling.LimitUD == -1)][['stkcd', 'score', 'weight']]
                toselllist = list(tosell.stkcd)
                weight_tosell = tosell.weight.sum()

                # 算出可买的股票
                buy_available = buying[(buying.status == 'Trading') & (buying.LimitUD != 1)]. \
                    sort_values('score', ascending=False)

                # 算出仓位不够买入的股票
                no_space_tobuy = buy_available[buy_available.weight.cumsum() +
                                               group[-group.stkcd.isin(buying.stkcd)].weight.sum() + weight_tosell > 1]

                # 待买的，两部分，一部分是不可交易的，另一部分是仓位不够买入的
                tobuylist = list(buying[(buying.status != 'Trading') | (buying.LimitUD == 1)].stkcd)
                tobuylist.extend(list(no_space_tobuy.stkcd))
                tobuy = buying[buying.stkcd.isin(tobuylist)][['stkcd', 'score', 'weight']]

                # 保留待卖的，删去待买的
                tosell_today = tosell.copy()
                tosell_today['time'] = today
                today_position = group[-group.stkcd.isin(tobuylist)]
                today_position['time'] = today
                today_position = today_position.append(tosell_today)
            else:
                # 普通交易日
                if len(tobuylist) == 0:
                    # 若tobuy为空，直接填充到月底或季度底
                    today_position = group.copy()
                    for i in period[period.index(today):]:
                        today_position['time'] = i
                        final_result = final_result.append(today_position)
                        yesterday_position = today_position.copy()
                    break
                else:
                    selling = pd.merge(tosell, status_limit[status_limit.time == today], on='stkcd')
                    # 若没有可交易的tosell，则无法买卖，直接跳出循环
                    if ((selling.status != 'Trading') | (selling.LimitUD == -1)).all():
                        pass
                    else:
                        tosell = selling[(selling.status != 'Trading') | (selling.LimitUD == -1)][
                            ['stkcd', 'score', 'weight']]
                        toselllist = list(tosell.stkcd)
                        # 计算出卖掉的权重
                        weight_sell = selling[-selling.stkcd.isin(toselllist)].weight.sum()

                        buying = pd.merge(tobuy, status_limit[status_limit.time == today], on='stkcd')
                        if ((buying.status != 'Trading') | (buying.LimitUD == 1)).all():
                            pass
                        else:
                            # 算出可买的股票
                            buy_available = buying[(buying.status == 'Trading') & (buying.LimitUD != 1)] \
                                .sort_values('score', ascending=False)
                            # 算出仓位不够买入的股票
                            no_space_tobuy = buy_available[buy_available.weight.cumsum() +
                                                           yesterday_position.weight.sum() - weight_sell > 1]
                            # 待买的，两部分，一部分是不可交易的，另一部分是仓位不够买入的
                            tobuylist = list(buying[(buying.status != 'Trading') | (buying.LimitUD == 1)].stkcd)
                            tobuylist.extend(list(no_space_tobuy.stkcd))
                            tobuy = buying[buying.stkcd.isin(tobuylist)][['stkcd', 'score', 'weight']]
                tosell_today = tosell.copy()
                tosell_today['time'] = today
                today_position = group[-group.stkcd.isin(tobuylist)]
                today_position['time'] = today
                today_position = today_position.append(tosell_today)
            # 每天最后把今日持仓写入final_result
            final_result = final_result.append(today_position)
            # 把今天赋值给昨天
            yesterday = today
            yesterday_position = today_position.copy()
    return final_result



def weighttoweight(change_position_day, factor_input_weight, factor):
    # 取得参数的deepcopy 防止污染参数数据
    change_position_day = change_position_day.copy()
    factor_input_weight = factor_input_weight.copy()

    final = change_position_day.copy()
    for i in range(len(factor_input_weight)):
        final.loc[:, factor[i]] = factor_input_weight[i]
    return final


def exclude_suspension(CPD_factor, trade_status):
    # 取得参数的deepcopy 防止污染参数数据
    CPD_factor = CPD_factor.copy()
    trade_status = trade_status.copy()

    result = pd.merge(CPD_factor, trade_status, on=["time", "stkcd"], how="left")
    result = result.dropna()
    result = result[result.status != "Suspension"]
    return result.drop("status", axis=1)
