import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import logging

class FinancialDatabase:
    def __init__(self, db_path='data/financial_data.db'):
        """初始化数据库连接"""
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.create_tables()
        logging.info(f"成功连接到数据库: {db_path}")
        
    def create_tables(self):
        cursor = self.conn.cursor()
        
        # 创建资产负债表
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS balance_sheet (
            序号 INTEGER,
            股票代码 TEXT,
            股票简称 TEXT,
            资产货币资金 REAL,
            资产应收账款 REAL,
            资产存货 REAL,
            资产总资产 REAL,
            资产总资产同比 REAL,
            负债应付账款 REAL,
            负债预收账款 REAL,
            负债总负债 REAL,
            负债总负债同比 REAL,
            资产负债率 REAL,
            股东权益合计 REAL,
            公告日期 TEXT,
            PRIMARY KEY (股票代码, 公告日期)
        )
        ''')

        # 创建利润表
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS income_statement (
            序号 INTEGER,
            股票代码 TEXT,
            股票简称 TEXT,
            净利润 REAL,
            净利润同比 REAL,
            营业总收入 REAL,
            营业总收入同比 REAL,
            营业总支出营业支出 REAL,
            营业总支出销售费用 REAL,
            营业总支出管理费用 REAL,
            营业总支出财务费用 REAL,
            营业总支出营业总支出 REAL,
            营业利润 REAL,
            利润总额 REAL,
            公告日期 TEXT,
            PRIMARY KEY (股票代码, 公告日期)
        )
        ''')

        # 创建现金流量表
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS cash_flow (
            序号 INTEGER,
            股票代码 TEXT,
            股票简称 TEXT,
            净现金流净现金流 REAL,
            净现金流同比增长 REAL,
            经营性现金流现金流量净额 REAL,
            经营性现金流净现金流占比 REAL,
            投资性现金流现金流量净额 REAL,
            投资性现金流净现金流占比 REAL,
            融资性现金流现金流量净额 REAL,
            融资性现金流净现金流占比 REAL,
            公告日期 TEXT,
            PRIMARY KEY (股票代码, 公告日期)
        )
        ''')

        self.conn.commit()
        logging.info("数据表创建成功")
            
    def save_balance_sheet(self, df):
        """保存资产负债表数据"""
        try:
            # 保存数据
            df.to_sql('balance_sheet', self.conn, if_exists='replace', index=False)
            logging.info(f"成功保存资产负债表数据")
        except Exception as e:
            logging.error(f"保存资产负债表数据失败: {str(e)}")
            raise
            
    def save_income_statement(self, df):
        """保存利润表数据"""
        try:
            # 保存数据
            df.to_sql('income_statement', self.conn, if_exists='replace', index=False)
            logging.info(f"成功保存利润表数据")
        except Exception as e:
            logging.error(f"保存利润表数据失败: {str(e)}")
            raise
            
    def save_cash_flow(self, df):
        """保存现金流量表数据"""
        try:
            # 保存数据
            df.to_sql('cash_flow', self.conn, if_exists='replace', index=False)
            logging.info(f"成功保存现金流量表数据")
        except Exception as e:
            logging.error(f"保存现金流量表数据失败: {str(e)}")
            raise
            
    def get_balance_sheet(self, stock_code=None, date=None):
        """获取资产负债表数据"""
        query = "SELECT * FROM balance_sheet"
        if stock_code:
            query += f" WHERE 股票代码 = '{stock_code}'"
        if date:
            query += f" AND 公告日期 = '{date}'" if stock_code else f" WHERE 公告日期 = '{date}'"
        cursor = self.conn.cursor()
        df = pd.read_sql_query(query, self.conn)
        return df
            
    def get_income_statement(self, stock_code=None, date=None):
        """获取利润表数据"""
        try:
            # 检查数据库连接
            if not self.conn:
                raise ConnectionError("数据库连接未建立")
                
            # 检查表是否存在
            cursor = self.conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='income_statement'")
            if not cursor.fetchone():
                raise ValueError("利润表不存在，请先导入数据")
                
            # 构建查询语句
            query = "SELECT * FROM income_statement"
            params = []
            
            if stock_code or date:
                query += " WHERE"
                conditions = []
                
                if stock_code:
                    conditions.append(" 股票代码 = ?")
                    params.append(stock_code)
                    
                if date:
                    # 标准化日期格式
                    try:
                        date = pd.to_datetime(date).strftime('%Y-%m-%d')
                        conditions.append(" 公告日期 = ?")
                        params.append(date)
                    except Exception as e:
                        logging.error(f"日期格式转换错误: {str(e)}")
                        raise ValueError(f"无效的日期格式: {date}")
                        
                query += " AND".join(conditions)
                
            logging.info(f"执行查询: {query}")
            logging.info(f"查询参数: {params}")
            
            # 执行查询
            df = pd.read_sql_query(query, self.conn, params=params)
            
            # 检查结果
            if df.empty:
                logging.warning(f"未找到利润表数据: stock_code={stock_code}, date={date}")
            else:
                logging.info(f"成功获取利润表数据，共 {len(df)} 条记录")
                
            return df
            
        except Exception as e:
            logging.error(f"获取利润表数据时出错: {str(e)}")
            raise
            
    def get_cash_flow(self, stock_code=None, date=None):
        """获取现金流量表数据"""
        query = "SELECT * FROM cash_flow"
        if stock_code:
            query += f" WHERE 股票代码 = '{stock_code}'"
        if date:
            query += f" AND 公告日期 = '{date}'" if stock_code else f" WHERE 公告日期 = '{date}'"
        cursor = self.conn.cursor()
        df = pd.read_sql_query(query, self.conn)
        return df
            
    def close(self):
        """关闭数据库连接"""
        if self.conn:
            self.conn.close()
            logging.info("数据库连接已关闭") 