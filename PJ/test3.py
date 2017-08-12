for today, group in trade_status.sort_values("time").groupby("time"):
    if yesterday in change_position_day:
        # 找到此调仓日的目标持仓current_CPD_position,今天的最终持仓根据此表来加减股票
        current_CPD_position = CPD_position_restrict[CPD_position_restrict.time == yesterday].copy()

        # 根据current_CPD_position和last_CPD_position的差异确定要卖的股票, 叫做selling
        selling = last_CPD_position[-last_CPD_position.stkcd.isin(current_CPD_position.stkcd)]
        selling = pd.merge(selling, group, on='stkcd')

        # 实际要卖的
        sell = list(selling.status == 'Trading')
        # 待卖的
        tosell = list(selling.status != 'Trading')
        # tosell卖不掉，所以这一天的position里应当保留tosell
        today_position = current_CPD_position[current_CPD_position.stkcd.isin(tosell)]
        weight_tosell = today_position.weight.sum()

        # 根据current_CPD_position和last_CPD_position的差异确定要买的股票，叫做buying
        buying = current_CPD_position[-current_CPD_position.stkcd.isin(last_CPD_position.stkcd)]
        buying = pd.merge(buying, group, on='stkcd')

        # 可买的，即不考虑仓位满不满且当天可交易的股票
        buy_available = buying[buying.status == 'Trading'].sort_values('score', ascending=False)
        # 计算仓位不够买入的股票
        no_space_tobuy = buy_available[buy_available.weight.cumsum() + weight_tosell > 1].stkcd
        # 待买的，两部分，一部分是不可交易的，另一部分是仓位不够买入的
        tobuy = list(buying[buying.status != 'Trading'].stkcd)
        tobuy.extend(list(no_space_tobuy))

        # 把实际买的加入今天的real_position里面
        today_position = today_position.append(current_CPD_position[-current_CPD_position.stkcd.isin(tobuy)])
        all_position = all_position.append(today_position)

        # 保存此换仓日以及今天的position
        last_CPD_position = current_CPD_position.copy()
        yesterday_position = today_position.copy()
        yesterday = today

    else:
        # 今天的目标持仓与yesterday_position一致, 今天的最终持仓根据此表来加减股票
        current_position = yesterday_position.copy()
        # 今天要卖、待买、实际要卖的股票
        selling = current_position[current_position.stkcd.isin(tosell)]
        selling = pd.merge(selling, group, on='stkcd')
        sell = list(selling[selling.status == 'Trading'].stkcd)
        tosell = list(selling[selling.status != 'Trading'].stkcd)

        if len(sell) == 0:
            today_position = current_position.copy()
            all_position = all_position.append(today_position)
            yesterday_position = today_position.copy()
            yesterday = today
        else:
            # 先删掉sell的部分
            today_position = current_position[-current_position.stkcd.isin(sell)]

            buying = last_CPD_position[last_CPD_position.stkcd.isin(tobuy)]
            buying = pd.merge(buying, group, on='stkcd')

            # 可买的，即不考虑仓位满不满且当天可交易的股票
            buy_available = buying[buying.status == 'Trading'].sort_values('score', ascending=False)
            if len(buy_available) == 0:
                all_position = all_position.append(today_position)
                yesterday_position = today_position.copy()
                yesterday = today
            else:
                # 计算仓位不够买入的股票
                no_space_tobuy = buy_available[buy_available.weight.cumsum() + today_position.weight.sum() > 1].stkcd
                # 待买的，两部分，一部分是不可交易的，另一部分是仓位不够买入的
                tobuy = list(buying[buying.status != 'Trading'].stkcd)
                tobuy.extend(list(no_space_tobuy))

                # 把实际买的加入今天的real_position里面
                today_position = today_position.append(buy_available[-buy_available.stkcd.isin(tobuy)])
                all_position = all_position.append(today_position)

                # 保存此换仓日以及今天的position
                yesterday_position = today_position.copy()
                yesterday = today