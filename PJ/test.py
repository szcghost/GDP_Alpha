# 调仓日后一天的list
# all_tradingday = list(all_tradingday.time)
# change_position_day = set(CPD_position_restrict.time)
# day_after_CPD = [all_tradingday[all_tradingday.index(i)+1] for i in change_position_day]
# day_after_CPD = sorted(day_after_CPD)

# # 遍历每天
# today_position = pd.DataFrame(columns=['time', 'stkcd', 'score', 'weight'])  # 存储今天的持仓
# yesterday_position = pd.DataFrame(columns=['time', 'stkcd', 'score', 'weight'])  # 存储昨天的持仓
# last_CPD_position = pd.DataFrame(columns=['time', 'stkcd', 'score', 'weight'])  # 存储上个调仓日的目标持仓
# yesterday = 0
# for today, group in trade_status.sort_values("time").groupby("time"):
#     if yesterday in change_position_day:
#         # 找到此调仓日的目标持仓current_CPD_position,今天的最终持仓根据此表来加减股票
#         current_CPD_position = CPD_position_restrict[CPD_position_restrict.time == yesterday].copy()
#
#         # 根据current_CPD_position和last_CPD_position的差异确定要卖的股票, 叫做selling
#         selling = last_CPD_position[-last_CPD_position.stkcd.isin(current_CPD_position)]
#         selling = pd.merge(selling, group, on='stkcd')
#
#         # 实际要卖的
#         sell = list(selling.status == 'Trading')
#         # 待卖的
#         tosell = list(selling.status != 'Trading')
#         # tosell卖不掉，所以这一天的position里应当保留tosell
#         today_position = current_CPD_position[current_CPD_position.stkcd.isin(tosell)]
#         weight_tosell = today_position.weight.sum()
#
#         # 根据current_CPD_position和last_CPD_position的差异确定要买的股票，叫做buying
#         buying = current_CPD_position[-current_CPD_position.stkcd.isin(last_CPD_position.stkcd)]
#         buying = pd.merge(buying, group, on='stkcd')
#
#         # 可买的，即不考虑仓位满不满且当天可交易的股票
#         buy_available = buying[buying.status == 'Trading'].sort_values('score', ascending=False)
#         # 计算仓位不够买入的股票
#         no_space_tobuy = buy_available[buy_available.weight.cumsum() + weight_tosell > 1].stkcd
#         # 待买的，两部分，一部分是不可交易的，另一部分是仓位不够买入的
#         tobuy = list(buying[buying.status != 'Trading'].stkcd)
#         tobuy.extend(list(no_space_tobuy))
#
#         # 把实际买的加入今天的real_position里面
#         today_position = today_position.append(current_CPD_position[-current_CPD_position.stkcd.isin(tobuy)])
#         all_position = all_position.append(today_position)
#
#         # 保存此换仓日以及今天的position
#         last_CPD_position = current_CPD_position.copy()
#         yesterday_position = today_position.copy()
#         yesterday = today
#
#     else:
#         # 今天的目标持仓与yesterday_position一致, 今天的最终持仓根据此表来加减股票
#         current_position = yesterday_position.copy()
#         # 今天要卖、待买、实际要卖的股票
#         selling = current_position[current_position.stkcd.isin(tosell)]
#         selling = pd.merge(selling, group, on='stkcd')
#         sell = list(selling[selling.status == 'Trading'].stkcd)
#         tosell = list(selling[selling.status != 'Trading'].stkcd)
#
#         if len(sell) == 0:
#             today_position = current_position.copy()
#             all_position = all_position.append(today_position)
#             yesterday_position = today_position.copy()
#             yesterday = today
#         else:
#             # 先删掉sell的部分
#             today_position = current_position[-current_position.stkcd.isin(sell)]
#
#             buying = last_CPD_position[last_CPD_position.stkcd.isin(tobuy)]
#             buying = pd.merge(buying, group, on='stkcd')
#
#             # 可买的，即不考虑仓位满不满且当天可交易的股票
#             buy_available = buying[buying.status == 'Trading'].sort_values('score', ascending=False)
#             if len(buy_available) == 0:
#                 all_position = all_position.append(today_position)
#                 yesterday_position = today_position.copy()
#                 yesterday = today
#             else:
#                 # 计算仓位不够买入的股票
#                 no_space_tobuy = buy_available[buy_available.weight.cumsum() + today_position.weight.sum() > 1].stkcd
#                 # 待买的，两部分，一部分是不可交易的，另一部分是仓位不够买入的
#                 tobuy = list(buying[buying.status != 'Trading'].stkcd)
#                 tobuy.extend(list(no_space_tobuy))
#
#                 # 把实际买的加入今天的real_position里面
#                 today_position = today_position.append(buy_available[-buy_available.stkcd.isin(tobuy)])
#                 all_position = all_position.append(today_position)
#
#                 # 保存此换仓日以及今天的position
#                 yesterday_position = today_position.copy()
#                 yesterday = today

result = pd.merge(all_trading_data, CPD_position_restrict, how='inner',
                  on=['stkcd', 'YYMM'], suffixes=['', '_huancang'])
CPD_position_restrict = CPD_position_restrict.sort_values("time")
result = result.sort_values("time")




final_result = pd.DataFrame()  # 最终仓位
last_month_position = pd.DataFrame(columns=result.columns)  # 上个月最终的仓位
yesterday = 0  # 存储昨天的日期
for (name1, group1), (name2, group2) in zip(CPD_position_restrict.groupby("time"), result.groupby("time_huancang")):
    group2 = group2.sort_values("time")
    for name3, group3 in group2.groupby("time"):
        today_position = pd.DataFrame()
        # 若今天是CPD，因为CPD当天持有的还是上个月的票，所以删除当天的持仓，换成上个月的持仓
        if name3 == name1:
            today_position = last_month_position.copy()
            today_position['time'] = len(today_position)*[name3]
        elif yesterday == name1:
            # 仅从上个月的position和这个月的position的差异得出的selling和buying
            selling = last_month_position[-last_month_position.stkcd.isin(group1.stkcd)]
            buying = group3[-group3.isin(last_month_position.stkcd)]

            # 查找buying和selling的交易状态
            selling = pd.merge(selling, trade_status, on=['time', 'stkcd'])
            buying = pd.merge(buying, trade_status, on=['time', 'stkcd'])

            # 确定tosell，并计算tosell的总权重
            tosell = selling[selling.status != 'Trading'][['stkcd', 'weight']]
            toselllist = list(tosell.stkcd)
            weigtht_tosell = selling[selling.stkcd.isin(toselllist)].weight.sum()

            # 算出可买的股票
            buy_available = buying[buying.status == 'Trading'].sort_values('score', ascending=False)

            # 算出仓位不够买入的股票
            no_space_tobuy = buy_available[buy_available.weight.cumsum() + weight_tosell > 1]

            # 待买的，两部分，一部分是不可交易的，另一部分是仓位不够买入的
            tobuylist = list(buying[buying.status != 'Trading'].stkcd)
            tobuylist.extend(list(no_space_tobuy.stkcd))


            tosell['time'] = len(today_position)*[name3]
            today_position = group3[-group3.isin(tobuylist)]
            today_position = today_position.append(tosell)
        else:
            if len(tobuylist) == 0:
                pass
            else:
                selling = pd.merge(tosell, trade_status[trade_status.time == name3], on='stkcd')
                # 若没有可交易的tosell，则无法买卖，直接跳出循环
                if (selling.status != 'Trading').all():
                    pass
                else:
                    tosell = selling[selling.status != 'Trading'][['stkcd', 'weight', 'score']]
                    toselllist = list(tosell.stkcd)
                    # 计算出卖掉的权重
                    weight_sell = selling[-selling.stkcd.isin(toselllist)].weight.sum()
                    buying = pd.merge(group3[group3.stkcd.isin(tobuylist)], trade_status, on=['time', 'stkcd'])
                    if (buying.status != 'Trading').all():
                        pass
                    else:
                        # 算出可买的股票
                        buy_available = buying[buying.status == 'Trading'].sort_values('score', ascending=False)
                        # 算出仓位不够买入的股票
                        no_space_tobuy = buy_available[buy_available.weight.cumsum() +
                                                       yesterday_weight - weight_sell > 1]
                        # 待买的，两部分，一部分是不可交易的，另一部分是仓位不够买入的
                        tobuylist = list(buying[buying.status != 'Trading'].stkcd)
                        tobuylist.extend(list(no_space_tobuy.stkcd))
            tosell['time'] = len(today_position)*[name3]
            today_position = group3[-group3.isin(tobuylist)]
            today_position = today_position.append(tosell)
        # 每天最后要做的赋值
        yesterday = name3
        yesterday_weight = today_position.weight.sum()
        final_result = final_result.append(today_position)
    # 每月最后要做的赋值
    last_month_position = today_position.copy()
