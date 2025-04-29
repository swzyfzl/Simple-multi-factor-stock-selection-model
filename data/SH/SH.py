from SH_model import StockDataModel
import time

def main():
    # 创建模型实例
    model = StockDataModel()
    
    print("开始获取上证指数数据...")
    
    # 获取数据
    df = model.fetch_sh_index_data()
    if df is not None:
        print(f"成功获取数据，共 {len(df)} 条记录")
        
        # 保存到数据库
        if model.save_to_db(df):
            print("数据已成功保存到数据库")
            
            # 显示最新的10条数据
            latest_data = model.get_latest_data(10)
            print("\n最新的10条数据：")
            for row in latest_data:
                print(f"日期: {row[0]}, 开盘: {row[1]}, 最高: {row[2]}, 最低: {row[3]}, 收盘: {row[4]}, 成交量: {row[5]}")
        else:
            print("保存数据失败")
    else:
        print("获取数据失败")

if __name__ == "__main__":
    main() 