import sys
import os
# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from test.database import StockDatabase
from test.data_loader import StockDataLoader
import pandas as pd
from datetime import datetime, timedelta

def check_database():
    """检查数据库内容"""
    db = StockDatabase()
    
    # 检查股票列表
    print("\n检查股票列表...")
    stock_list = db.get_stock_list()
    if stock_list.empty:
        print("数据库中没有股票列表数据")
    else:
        print(f"数据库中有 {len(stock_list)} 只股票")
        print(stock_list.head())
    
    # 检查股票数据
    print("\n检查股票数据...")
    if not stock_list.empty:
        code = stock_list.iloc[0]['code']
        data = db.get_stock_data(code)
        if data is None:
            print(f"股票 {code} 没有数据")
        else:
            print(f"股票 {code} 的数据：")
            print(f"数据时间范围：{data.index[0]} 到 {data.index[-1]}")
            print(f"数据条数：{len(data)}")
            print(data.head())

def main():
    # 创建数据库实例
    db = StockDatabase()
    
    # 创建数据加载器
    loader = StockDataLoader()
    
    # 先检查数据库内容
    check_database()
    
    # 示例1：获取股票列表
    print("\n示例1：获取股票列表")
    stock_list = loader.get_stock_list()
    print(f"获取到 {len(stock_list)} 只股票")
    print(stock_list.head())
    print("\n")
    

    # 更新股票数据  
    print("\n更新股票数据")
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=365*3)).strftime('%Y%m%d')
    stock_data = loader.get_stock_data(start_date=start_date, end_date=end_date)
    print(f"获取到 {len(stock_data)} 只股票的数据")
    
    # 再次检查数据库内容
    print("\n再次检查数据库内容...")
    check_database()

if __name__ == "__main__":
    main() 