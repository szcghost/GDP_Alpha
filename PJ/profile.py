from pycallgraph import PyCallGraph
from pycallgraph import Config
from pycallgraph.output import GraphvizOutput

from backtest import *

graphviz = GraphvizOutput(output_file=r'./trace_detail.png')
with PyCallGraph(output=graphviz):
    para_dict = {
        "path": 'D:\strategy\GDP\GD.db',
        "factor": ["pb"],
        "start": "2010-01-01",
        "end": "2014-01-01",
        "freq": "M",
        "weightType": 1,
        "positionType": 1,
        "backperiod": 20
    }
    # weightType = 1  # 因子打分权重算法（1是普通加权，2是主成分分析，3是逐步回归，4是广义最小二乘）
    # positionType = 1  # 资产组合构成算法，1是普通加权，2是30天股价波动率加权， 3是市值加权
    # backperiod = 20  # 因子权重计算过程中，使用多少过去的数据来计算调仓日的因子打分权重
    backtest = BackTest(para_dict)
    backtest.run()
    # backtest.trade_position.to_csv("position_trade.csv")
    # backtest.final_position.to_csv("position_final.csv")
    print(backtest.daily_return)
    print(backtest.portfolio)
    # pickle.dump(backtest.final_position,open("final_position","wb"))
    # pickle.dump(backtest.all_trading_data,open("trading_data","wb"))