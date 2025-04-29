import pandas as pd
import sqlite3
from datetime import datetime
import os

def calculate_sh_returns(start_date, end_date):
    """
    计算上证指数的每日变化率并保存为CSV文件
    
    Args:
        start_date: 开始日期，格式为'YYYY-MM-DD'
        end_date: 结束日期，格式为'YYYY-MM-DD'
    """
    try:
        # 获取当前脚本所在目录
        current_dir = os.path.dirname(os.path.abspath(__file__))
        db_path = os.path.join(current_dir, 'SH_data.db')
        
        # 连接数据库
        conn = sqlite3.connect(db_path)
        
        # 读取数据
        query = """
        SELECT date, close
        FROM sh_index
        WHERE date BETWEEN ? AND ?
        ORDER BY date
        """
        sh_data = pd.read_sql_query(query, conn, params=[start_date, end_date])
        
        # 关闭数据库连接
        conn.close()
        
        if sh_data.empty:
            print(f"错误：在 {start_date} 到 {end_date} 期间没有找到上证指数数据")
            return
        
        # 转换日期格式
        sh_data['date'] = pd.to_datetime(sh_data['date'])
        
        # 计算每日变化率
        sh_data['daily_return'] = sh_data['close'].pct_change() * 100
        
        # 计算累计收益率
        sh_data['cumulative_return'] = ((1 + sh_data['daily_return']/100).cumprod() - 1) * 100
        
        # 重命名列
        sh_data = sh_data.rename(columns={
            'date': '日期',
            'close': '收盘价',
            'daily_return': '日收益率(%)',
            'cumulative_return': '累计收益率(%)'
        })
        
        # 保存为CSV文件
        output_file = os.path.join(current_dir, f'sh_returns.csv')
        sh_data.to_csv(output_file, index=False, encoding='utf-8-sig')
        
        print(f"上证指数收益率数据已保存到 {output_file}")
        print(f"数据期间：{sh_data['日期'].min().strftime('%Y-%m-%d')} 至 {sh_data['日期'].max().strftime('%Y-%m-%d')}")
        print(f"总交易日数：{len(sh_data)}")
        print(f"平均日收益率：{sh_data['日收益率(%)'].mean():.2f}%")
        print(f"最大日收益率：{sh_data['日收益率(%)'].max():.2f}%")
        print(f"最小日收益率：{sh_data['日收益率(%)'].min():.2f}%")
        print(f"最终累计收益率：{sh_data['累计收益率(%)'].iloc[-1]:.2f}%")
        
    except Exception as e:
        print(f"计算上证指数收益率时出错：{str(e)}")

if __name__ == "__main__":
    # 示例使用
    start_date = '2023-01-01'
    end_date = '2023-12-31'
    calculate_sh_returns(start_date, end_date) 