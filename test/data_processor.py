import pandas as pd
import numpy as np
from typing import Dict, List, Tuple
from datetime import datetime, timedelta
from database import StockDatabase
from financial_database import FinancialDatabase
import os
import logging

class DataProcessor:
    def __init__(self):
        # 初始化历史行情数据库
        self.stock_db = StockDatabase('data/stock_data.db')
        # 初始化财务数据库
        self.financial_db = FinancialDatabase('data/financial_data.db')
        self.stock_data = {}
        self.market_data = {}
        self.financial_data = {}
        
    def filter_stocks(self, stock_list: pd.DataFrame) -> pd.DataFrame:
        """筛选股票，剔除ST股票和创业板股票"""
        # 剔除创业板股票（代码以30开头）
        filtered_stocks = stock_list[~stock_list['code'].str.startswith('30')]
        
        # 剔除ST股票（名字中包含ST）
        filtered_stocks = filtered_stocks[~filtered_stocks['name'].str.contains('ST', case=False)]
        
        return filtered_stocks
        
    def get_stock_data(self, codes: List[str], start_date: str, end_date: str) -> Dict[str, pd.DataFrame]:
        """从历史行情数据库获取股票数据"""
        stock_data = {}
        for code in codes:
            data = self.stock_db.get_stock_data(code, start_date, end_date)
            if data is not None and not data.empty:
                stock_data[code] = data
        return stock_data
        
    def get_market_data(self, start_date: str, end_date: str) -> Dict[str, pd.DataFrame]:
        """从历史行情数据库获取市场数据（使用上证指数作为市场基准）"""
        market_data = {}
        market_code = '000001'  # 上证指数代码
        data = self.stock_db.get_stock_data(market_code, start_date, end_date)
        if data is not None and not data.empty:
            market_data[market_code] = data
        return market_data
        
    def get_financial_data(self, codes: List[str], date: str) -> Dict[str, pd.DataFrame]:
        """从财务数据库获取财务数据"""
        financial_data = {}
        for code in codes:
            try:
                logging.info(f"开始获取股票 {code} 的财务数据")
                
                # 获取资产负债表
                balance_sheet = self.financial_db.get_balance_sheet(code, date)
                if balance_sheet is None or balance_sheet.empty:
                    logging.warning(f"股票 {code} 的资产负债表数据为空")
                    continue
                    
                # 获取利润表
                income_statement = self.financial_db.get_income_statement(code, date)
                if income_statement is None or income_statement.empty:
                    logging.warning(f"股票 {code} 的利润表数据为空")
                    continue
                    
                # 获取现金流量表
                cash_flow = self.financial_db.get_cash_flow(code, date)
                if cash_flow is None or cash_flow.empty:
                    logging.warning(f"股票 {code} 的现金流量表数据为空")
                    continue
                
                # 合并财务数据
                try:
                    financial_data[code] = pd.concat([
                        balance_sheet,
                        income_statement,
                        cash_flow
                    ], axis=1)
                    logging.info(f"成功合并股票 {code} 的财务数据")
                except Exception as e:
                    logging.error(f"合并股票 {code} 的财务数据时出错: {str(e)}")
                    continue
                    
            except Exception as e:
                logging.error(f"获取股票 {code} 的财务数据时出错: {str(e)}")
                continue
                
        if not financial_data:
            logging.warning(f"未找到任何股票的财务数据")
            
        return financial_data
        
    def process_data(self, start_date: str, end_date: str) -> Tuple[Dict[str, pd.DataFrame], Dict[str, pd.DataFrame], Dict[str, pd.DataFrame]]:
        """处理所有数据"""
        try:
            # 获取股票列表
            stock_list = self.stock_db.get_stock_list()
            if stock_list.empty:
                raise ValueError("没有获取到股票列表")
                
            # 筛选股票
            filtered_stocks = self.filter_stocks(stock_list)
            codes = filtered_stocks['code'].tolist()
            
            # 获取股票数据
            self.stock_data = self.get_stock_data(codes, start_date, end_date)
            
            # 获取市场数据
            self.market_data = self.get_market_data(start_date, end_date)
            
            # 获取财务数据
            self.financial_data = self.get_financial_data(codes, end_date)
            
            return self.stock_data, self.market_data, self.financial_data
            
        except Exception as e:
            print(f"数据处理出错: {str(e)}")
            return {}, {}, {}
            
    def get_processed_data(self) -> Tuple[Dict[str, pd.DataFrame], Dict[str, pd.DataFrame], Dict[str, pd.DataFrame]]:
        """获取处理后的数据"""
        return self.stock_data, self.market_data, self.financial_data
        
    def close(self):
        """关闭数据库连接"""
        if hasattr(self, 'stock_db'):
            self.stock_db.close()
        if hasattr(self, 'financial_db'):
            self.financial_db.close() 