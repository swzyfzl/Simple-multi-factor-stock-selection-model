import sys
import os
# 添加项目根目录到Python路径
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)

# 修改导入语句
from database import StockDatabase
from data_loader import StockDataLoader
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

def update_all_stocks(db, loader, stock_list, start_date, end_date):
    """更新所有股票的数据"""
    print(f"\n开始更新 {len(stock_list)} 只股票的数据...")
    print(f"更新日期范围：{start_date} 到 {end_date}")
    
    for _, row in stock_list.iterrows():
        code = row['code']
        try:
            # 获取数据库中该股票的最新日期
            data = db.get_stock_data(code)
            if data is not None and not data.empty:
                last_date = data.index[-1]
                # 如果数据库中已有最新数据，则不需要更新
                if last_date >= pd.to_datetime(end_date):
                    print(f"股票 {code} 的数据已是最新，无需更新")
                    continue
                
                # 只获取数据库中不存在的最新数据
                start_date = (last_date + timedelta(days=1)).strftime('%Y%m%d')
                print(f"股票 {code} 将从 {start_date} 开始更新")
            
            # 获取新数据
            print(f"获取股票 {code} 的数据...")
            df = loader.get_stock_data(start_date=start_date, end_date=end_date)
            if code in df and not df[code].empty:
                # 保存新数据
                db.save_stock_data(code, df[code])
                print(f"成功更新股票 {code} 的数据，新增 {len(df[code])} 条记录")
            else:
                print(f"股票 {code} 没有新数据需要更新")
        except Exception as e:
            print(f"更新股票 {code} 数据时出错: {str(e)}")

def main():
    # 创建数据库实例
    db = StockDatabase()
    
    # 创建数据加载器
    loader = StockDataLoader()
    
    # 先检查数据库内容
    check_database()
    
    # 获取股票列表
    print("\n获取股票列表...")
    stock_list = loader.get_stock_list()
    if stock_list.empty:
        print("没有获取到股票列表")
        return
    
    print(f"获取到 {len(stock_list)} 只股票")
    print(stock_list.head())
    
    # 设置更新日期范围
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = '20200101'  # 从2020年开始获取数据
    
    # 更新所有股票的数据
    update_all_stocks(db, loader, stock_list, start_date, end_date)
    
    # 再次检查数据库内容
    print("\n再次检查数据库内容...")
    check_database()

if __name__ == "__main__":
    main() 