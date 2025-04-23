import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

class StockDatabase:
    def __init__(self, db_path='stock_data.db'):
        self.db_path = db_path
        self._init_db()
        self.stock_data = {}
        
    def _init_db(self):
        """初始化数据库，创建必要的表"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 创建股票列表表
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS stock_list (
            code TEXT PRIMARY KEY,
            name TEXT,
            update_time TIMESTAMP
        )
        ''')
        
        # 创建股票数据表
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS stock_data (
            code TEXT,
            date DATE,
            开盘 REAL,
            收盘 REAL,
            最高 REAL,
            最低 REAL,
            成交量 REAL,
            成交额 REAL,
            update_time TIMESTAMP,
            PRIMARY KEY (code, date)
        )
        ''')
        
        conn.commit()
        conn.close()
        
    def save_stock_list(self, stock_list):
        """保存股票列表"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 清空现有数据
        cursor.execute('DELETE FROM stock_list')
        
        # 插入新数据
        current_time = datetime.now()
        for _, row in stock_list.iterrows():
            cursor.execute('''
            INSERT OR REPLACE INTO stock_list (code, name, update_time)
            VALUES (?, ?, ?)
            ''', (row['code'], row['name'], current_time))
            
        conn.commit()
        conn.close()
        
    def get_stock_list(self):
        """获取股票列表"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('SELECT code, name FROM stock_list')
        rows = cursor.fetchall()
        conn.close()
        
        if not rows:
            return pd.DataFrame(columns=['code', 'name'])
            
        return pd.DataFrame(rows, columns=['code', 'name'])
        
    def save_stock_data(self, code, data):
        """保存单只股票的数据"""
        conn = sqlite3.connect(self.db_path)
        current_time = datetime.now()
        
        # 将数据转换为适合插入的格式
        data = data.copy()  # 创建副本避免修改原始数据
        
        # 确保数据有正确的列名
        if '日期' in data.columns:
            data = data.rename(columns={'日期': 'date'})
        
        # 添加必要的列
        data['code'] = code
        data['update_time'] = current_time
        
        # 确保所有必要的列都存在
        required_columns = ['date', '开盘', '收盘', '最高', '最低', '成交量', '成交额', 'code', 'update_time']
        for col in required_columns:
            if col not in data.columns:
                print(f"警告: 数据中缺少列 '{col}'，将使用默认值")
                if col == 'date':
                    data[col] = data.index
                else:
                    data[col] = 0
        
        # 先删除该股票的所有数据
        cursor = conn.cursor()
        cursor.execute('DELETE FROM stock_data WHERE code = ?', (code,))
        conn.commit()
        
        # 插入新数据
        try:
            data.to_sql('stock_data', conn, if_exists='append', index=False)
            print(f"成功保存股票 {code} 的数据，共 {len(data)} 条记录")
        except Exception as e:
            print(f"保存股票 {code} 数据时出错: {str(e)}")
            # 尝试打印数据的前几行，帮助调试
            print("数据预览:")
            print(data.head())
        
        conn.close()
        
    def get_stock_data(self, code, start_date=None, end_date=None):
        """获取股票数据"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 构建查询条件
            query = 'SELECT * FROM stock_data WHERE code = ?'
            params = [code]
            
            if start_date:
                query += ' AND date >= ?'
                params.append(start_date)
            if end_date:
                query += ' AND date <= ?'
                params.append(end_date)
                
            query += ' ORDER BY date'
            
            # 执行查询
            cursor.execute(query, params)
            rows = cursor.fetchall()
            conn.close()
            
            if not rows:
                print(f"警告: 股票 {code} 在数据库中没有数据")
                return None
                
            # 获取列名
            columns = ['code', 'date', '开盘', '收盘', '最高', '最低', '成交量', '成交额', 'update_time']
            
            # 创建DataFrame
            df = pd.DataFrame(rows, columns=columns)
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)
            
            # 删除不需要的列
            df = df.drop(['code', 'update_time'], axis=1)
            
            print(f"成功从数据库获取股票 {code} 的数据，共 {len(df)} 条记录")
            return df
            
        except Exception as e:
            print(f"从数据库获取股票 {code} 数据时出错: {str(e)}")
            return None
        
    def is_data_available(self, code, start_date, end_date):
        """检查指定时间段的数据是否已存在"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
        SELECT COUNT(*) FROM stock_data 
        WHERE code = ? AND date BETWEEN ? AND ?
        ''', (code, start_date, end_date))
        
        count = cursor.fetchone()[0]
        conn.close()
        
        return count > 0 