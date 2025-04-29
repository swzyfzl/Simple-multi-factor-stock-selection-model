import sys
import os
# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import StockDatabase
from factor_model import FactorModel
from backtest import Backtest
from data_loader import StockDataLoader
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from typing import Dict, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import logging

# 配置日志记录
def setup_logging():
    """
    配置日志记录，将日志保存到文件中
    """
    # 创建logs目录（如果不存在）
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
    # 生成日志文件名，包含时间戳
    log_filename = f"logs/backtest_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    
    # 配置日志记录器
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler()  # 同时输出到控制台
        ]
    )
    
    return log_filename

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
    # 计算总收益率
    total_return = (1 + returns).prod() - 1
    
    # 计算年化收益率
    annual_return = (1 + total_return) ** (252 / len(returns)) - 1
    
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

def filter_stocks(stock_list: pd.DataFrame) -> pd.DataFrame:
    """
    筛选股票，剔除ST股票、创业板股票和科创板股票
    
    Args:
        stock_list: 股票列表DataFrame，包含code和name列
    
    Returns:
        筛选后的股票列表DataFrame
    """
    # 剔除创业板股票（代码以30开头）
    filtered_stocks = stock_list[~stock_list['code'].str.startswith('30')]
    
    # 剔除科创板股票（代码以68开头）
    filtered_stocks = filtered_stocks[~filtered_stocks['code'].str.startswith('68')]
    
    # 剔除ST股票（名字中包含ST）
    filtered_stocks = filtered_stocks[~filtered_stocks['name'].str.contains('ST', case=False)]
    
    return filtered_stocks

def process_stock_data(args):
    """
    处理单只股票的数据
    
    Args:
        args: 包含数据库连接、股票代码、开始日期和结束日期的元组
    
    Returns:
        处理结果字典，包含股票代码和处理后的数据
    """
    db, code, start_date, end_date = args
    try:
        # 获取所有历史数据
        all_data = db.get_stock_data(code, None, end_date)
        if all_data is not None and not all_data.empty:
            # 按时间排序
            all_data = all_data.sort_index()
            
            # 获取最近一年的数据作为测试集
            test_data = all_data[all_data.index >= start_date]
            # 获取之前的所有数据作为训练集
            train_data = all_data[all_data.index < start_date]
            
            if len(train_data) > 0 and len(test_data) > 0:
                # 记录获取的数据信息到日志
                logging.info(f"成功从数据库获取股票 {code} 的数据，共 {len(all_data)} 条记录")
                
                return {
                    'code': code,
                    'data': all_data,
                    'train': train_data,
                    'test': test_data,
                    'success': True,
                    'message': None
                }
    except Exception as e:
        logging.error(f"处理股票 {code} 数据时出错: {str(e)}")
        return {
            'code': code,
            'success': False,
            'message': None
        }
    return {
        'code': code,
        'success': False,
        'message': None
    }

def main():
    try:
        # 设置日志记录
        log_filename = setup_logging()
        logging.info("开始回测程序")
        
        # 初始化数据库连接
        logging.info("正在连接数据库...")
        db = StockDatabase()
        
        # 设置回测参数
        end_date = datetime.now()
        start_date = datetime(2024, 1, 1)  # 从2024年1月1日开始
        logging.info(f"测试期间：{start_date.strftime('%Y-%m-%d')} 至 {end_date.strftime('%Y-%m-%d')}")
        logging.info("训练数据：使用所有历史数据")
        
        # 获取股票列表
        logging.info("正在获取股票列表...")
        stock_list = db.get_stock_list()
        if stock_list.empty:
            raise ValueError("没有获取到股票列表，请先运行 data_loader.py 导入数据")
        logging.info(f"获取到 {len(stock_list)} 只股票")
        
        # 筛选股票
        stock_list = filter_stocks(stock_list)
        logging.info(f"剔除ST股票和创业板股票后剩余 {len(stock_list)} 只股票")
        
        # 准备并行处理参数
        process_args = [(db, code, start_date, end_date) for code in stock_list['code']]
        
        # 使用线程池并行处理股票数据
        logging.info("正在并行处理股票数据...")
        stock_data = {}
        train_data = {}
        test_data = {}
        success_count = 0
        
        with ThreadPoolExecutor(max_workers=min(32, len(stock_list))) as executor:
            # 使用tqdm显示进度条
            futures = [executor.submit(process_stock_data, args) for args in process_args]
            for future in tqdm(as_completed(futures), total=len(futures), desc="处理进度"):
                result = future.result()
                if result['success']:
                    code = result['code']
                    stock_data[code] = result['data']
                    train_data[code] = result['train']
                    test_data[code] = result['test']
                    success_count += 1
        
        if not stock_data:
            raise ValueError("没有获取到有效的股票数据")
        logging.info(f"成功处理 {success_count} 只股票的数据")
        
        # 初始化因子模型
        logging.info("开始因子计算...")
        factor_model = FactorModel()
        
        try:
            # 添加因子及其权重
            factor_model.add_factor('rsi', 0.4)  # RSI因子，权重0.4
            factor_model.add_factor('volatility', 0.3)  # 波动率因子，权重0.3
            factor_model.add_factor('momentum', 0.3)  # 动量因子，权重0.3
            
            # 使用训练集数据计算所有因子
            logging.info("计算技术因子...")
            factor_model.calculate_technical_factors(train_data, end_date.strftime('%Y-%m-%d'))
            
            # 计算最终得分
            logging.info("计算最终得分...")
            final_scores = factor_model.calculate_final_score()
            
            # 选择得分最高的3只股票
            top_stocks = final_scores.nlargest(3)
            logging.info("选出的股票：")
            for code, score in top_stocks.items():
                stock_name = stock_list[stock_list['code'] == code]['name'].iloc[0]
                logging.info(f"股票 {code} ({stock_name}): 得分 {score:.4f}")
            
            # 使用测试集数据进行回测
            logging.info("开始回测...")
            backtest = Backtest(
                stock_data=test_data,  # 传入所有可交易的股票数据
                start_date=test_data[list(test_data.keys())[0]].index[0],
                end_date=test_data[list(test_data.keys())[0]].index[-1],
                factor_model=factor_model,
                rebalance_period=20,  # 每月调仓（约20个交易日）
                top_n=3
            )
            
            # 运行回测
            backtest.run_backtest()
            
            # 计算回测指标
            returns = pd.Series(backtest.returns)
            metrics = calculate_performance_metrics(returns)
            
            # 输出回测结果
            logging.info("回测结果：")
            logging.info(f"年化收益率: {metrics['annual_return']:.2%}")
            logging.info(f"年化波动率: {metrics['annual_volatility']:.2%}")
            logging.info(f"夏普比率: {metrics['sharpe_ratio']:.2f}")
            logging.info(f"最大回撤: {metrics['max_drawdown']:.2%}")
            
            # 生成回测报告
            backtest.generate_report()
            logging.info(f"回测报告已生成到 backtest_report.html")
            logging.info(f"日志已保存到 {log_filename}")
            
        except Exception as e:
            logging.error(f"因子计算过程中出错: {str(e)}")
            raise
        
    except Exception as e:
        logging.error(f"错误：{str(e)}")
        return

def update_stock_data():
    """更新股票数据"""
    logging.info("开始更新股票数据...")
    
    # 创建数据库实例
    db = StockDatabase()
    
    # 创建数据加载器
    loader = StockDataLoader()
    
    # 获取股票列表
    logging.info("获取股票列表...")
    stock_list = loader.get_stock_list()
    if stock_list.empty:
        logging.error("没有获取到股票列表")
        return False
    
    logging.info(f"获取到 {len(stock_list)} 只股票")
    
    # 设置更新日期范围
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = '20240101'  # 从2024年1月1日开始获取数据
    
    # 更新所有股票的数据
    logging.info(f"更新日期范围：{start_date} 到 {end_date}")

if __name__ == "__main__":
    main() 