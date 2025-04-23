import sys
import os
# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import StockDatabase
from factor_model import FactorModel
from backtest import Backtest
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from typing import Dict, Tuple

def split_data(data: pd.DataFrame, test_days: int = 252) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    将数据按时间顺序划分为训练集和测试集
    
    Args:
        data: 原始数据
        test_days: 测试集天数，默认252个交易日（约一年）
    
    Returns:
        train_data: 训练集数据
        test_data: 测试集数据
    """
    if len(data) <= test_days:
        raise ValueError(f"数据长度不足{test_days}天，无法进行划分")
    return data.iloc[:-test_days], data.iloc[-test_days:]

def calculate_performance_metrics(returns: pd.Series) -> Dict[str, float]:
    """
    计算回测性能指标
    
    Args:
        returns: 日收益率序列
    
    Returns:
        包含各项指标的字典
    """
    # 计算年化收益率
    annual_return = (1 + returns.mean()) ** 252 - 1
    
    # 计算年化波动率
    annual_volatility = returns.std() * np.sqrt(252)
    
    # 计算夏普比率（假设无风险利率为3%）
    risk_free_rate = 0.03
    sharpe_ratio = (annual_return - risk_free_rate) / annual_volatility if annual_volatility != 0 else 0
    
    # 计算最大回撤
    cummax = (1 + returns).cumprod().cummax()
    drawdown = (1 + returns).cumprod() / cummax - 1
    max_drawdown = drawdown.min()
    
    return {
        'annual_return': annual_return,
        'annual_volatility': annual_volatility,
        'sharpe_ratio': sharpe_ratio,
        'max_drawdown': max_drawdown
    }

def main():
    try:
        # 初始化数据库连接
        print("正在连接数据库...")
        db = StockDatabase()
        
        # 设置回测参数
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365*3)  # 获取三年的数据
        print(f"数据期间：{start_date.strftime('%Y-%m-%d')} 至 {end_date.strftime('%Y-%m-%d')}")
        
        # 获取股票列表
        print("\n正在获取股票列表...")
        stock_list = db.get_stock_list()
        if stock_list.empty:
            raise ValueError("没有获取到股票列表，请先运行 data_loader.py 导入数据")
        print(f"成功获取 {len(stock_list)} 只股票")
        
        # 获取并处理股票数据
        print("\n正在获取股票数据...")
        stock_data = {}
        train_data = {}
        test_data = {}
        
        for code in stock_list['code']:
            try:
                data = db.get_stock_data(code, start_date, end_date)
                if data is not None and not data.empty:
                    # 按时间排序
                    data = data.sort_index()
                    # 划分训练集和测试集
                    try:
                        train, test = split_data(data)
                        if len(train) > 0 and len(test) > 0:
                            stock_data[code] = data
                            train_data[code] = train
                            test_data[code] = test
                            print(f"成功处理股票 {code} 的数据，训练集 {len(train)} 天，测试集 {len(test)} 天")
                    except ValueError as e:
                        print(f"警告：股票 {code} 数据不足一年，跳过")
                        continue
            except Exception as e:
                print(f"警告：处理股票 {code} 数据时出错: {str(e)}")
                continue
        
        if not stock_data:
            raise ValueError("没有获取到有效的股票数据")
        print(f"\n成功处理 {len(stock_data)} 只股票的数据")
        
        # 初始化因子模型
        print("\n开始因子计算...")
        factor_model = FactorModel()
        
        # 添加因子及其权重
        factor_model.add_factor('rsi', 0.3)
        factor_model.add_factor('volatility', 0.3)
        factor_model.add_factor('momentum', 0.4)
        
        # 使用训练集数据计算因子
        factor_model.calculate_technical_factors(train_data)
        final_scores = factor_model.calculate_final_score()
        
        # 选择得分最高的3只股票
        top_stocks = final_scores.nlargest(3)
        print("\n选出的股票：")
        for code, score in top_stocks.items():
            print(f"股票 {code}: 得分 {score:.4f}")
        
        # 使用测试集数据进行回测
        print("\n开始回测...")
        backtest = Backtest(
            stock_data={code: test_data[code] for code in top_stocks.index},
            start_date=test_data[list(test_data.keys())[0]].index[0],
            end_date=test_data[list(test_data.keys())[0]].index[-1],
            factor_model=factor_model,
            rebalance_period=20,
            top_n=3
        )
        
        # 运行回测
        backtest.run_backtest()
        
        # 计算回测指标
        returns = pd.Series(backtest.returns)
        metrics = calculate_performance_metrics(returns)
        
        # 输出回测结果
        print("\n回测结果：")
        print(f"年化收益率: {metrics['annual_return']:.2%}")
        print(f"年化波动率: {metrics['annual_volatility']:.2%}")
        print(f"夏普比率: {metrics['sharpe_ratio']:.2f}")
        print(f"最大回撤: {metrics['max_drawdown']:.2%}")
        
        # 生成回测报告
        backtest.generate_report()
        print("\n回测报告已生成到 backtest_report.html")
        
    except Exception as e:
        print(f"\n错误：{str(e)}")
        return

if __name__ == "__main__":
    main() 