import pandas as pd
import numpy as np
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib import font_manager
import sqlite3
import os

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号

class Backtest:
    def __init__(self, stock_data, start_date, end_date, factor_model=None, rebalance_period=20, top_n=3):
        """
        初始化回测类
        
        Args:
            stock_data: 股票数据字典，key为股票代码，value为DataFrame
            start_date: 回测开始日期
            end_date: 回测结束日期
            factor_model: 因子模型实例
            rebalance_period: 调仓周期（交易日），默认20天（约一个月）
            top_n: 每次选择的股票数量
        """
        self.stock_data = stock_data
        self.start_date = start_date
        self.end_date = end_date
        self.factor_model = factor_model
        self.rebalance_period = rebalance_period  # 调仓周期（交易日）
        self.top_n = top_n  # 每次选择的股票数量
        self.portfolio_value = []
        self.returns = []
        self.positions = {}  # 记录持仓情况
        self.trade_history = []  # 记录交易历史
        self.cash = 50000  # 初始资金
        self.commission_rate_buy = 0.0001  # 买入手续费率
        self.commission_rate_sell = 0.0003  # 卖出手续费率
        self.min_commission = 5  # 最低手续费
        self.slippage = 0.0002  # 滑点
        
        # 计算上证指数收益率
        self.calculate_sh_returns()
        
    def calculate_sh_returns(self):
        """计算上证指数的每日变化率并保存为CSV文件"""
        try:
            # 获取当前脚本所在目录
            current_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            db_path = os.path.join(current_dir, 'data', 'SH', 'SH_data.db')
            
            # 连接数据库
            conn = sqlite3.connect(db_path)
            
            # 读取数据
            query = """
            SELECT date, close
            FROM sh_index
            WHERE date BETWEEN ? AND ?
            ORDER BY date
            """
            sh_data = pd.read_sql_query(query, conn, 
                                      params=[self.start_date.strftime('%Y-%m-%d'), 
                                             self.end_date.strftime('%Y-%m-%d')])
            
            # 关闭数据库连接
            conn.close()
            
            if sh_data.empty:
                print(f"错误：在 {self.start_date} 到 {self.end_date} 期间没有找到上证指数数据")
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
            output_file = os.path.join(current_dir, 'data', 'SH', 'sh_returns.csv')
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
        
    def calculate_commission(self, value, is_buy=True):
        """计算手续费"""
        rate = self.commission_rate_buy if is_buy else self.commission_rate_sell
        commission = value * rate
        return max(commission, self.min_commission)
        
    def apply_slippage(self, price, is_buy=True):
        """应用滑点"""
        if is_buy:
            return price * (1 + self.slippage)
        else:
            return price * (1 - self.slippage)
            
    def run_backtest(self):
        """运行回测"""
        # 初始化投资组合
        self.cash = 50000  # 初始资金
        portfolio_value = self.cash  # 初始资金
        self.portfolio_value = [portfolio_value]  # 重置投资组合价值列表
        
        # 检查股票数据是否为空
        if not self.stock_data:
            print("错误：没有股票数据可供回测")
            return
            
        print(f"开始回测，共有 {len(self.stock_data)} 只股票")
        
        # 获取日期范围
        date_range = pd.date_range(start=self.start_date, end=self.end_date)
        print(f"回测日期范围：{date_range[0]} 至 {date_range[-1]}，共 {len(date_range)} 个交易日")
        
        # 初始化持仓
        self.positions = {}  # 记录持仓数量
        self.position_values = {}  # 记录持仓市值
        self.returns = []  # 重置收益率列表
        
        # 计算每日收益率
        for i, date in enumerate(date_range):
            # 检查是否需要调仓
            if i % self.rebalance_period == 0 and self.factor_model is not None:
                # 获取当前可用的股票数据
                available_stocks = {}
                for code, data in self.stock_data.items():
                    if date in data.index:
                        # 获取到当前日期为止的所有历史数据
                        historical_data = data[data.index <= date].copy()
                        if not historical_data.empty:
                            available_stocks[code] = historical_data
                
                if available_stocks:
                    # 使用历史数据重新计算因子
                    self.factor_model.calculate_technical_factors(available_stocks)
                    final_scores = self.factor_model.calculate_final_score()
                    selected_stocks = [(code, score) for code, score in final_scores.items()]
                    selected_stocks.sort(key=lambda x: x[1], reverse=True)
                    selected_stocks = selected_stocks[:self.top_n]
                    
                    # 记录选股结果
                    selected_codes = [code for code, _ in selected_stocks]
                    print(f"日期 {date.strftime('%Y-%m-%d')} 选出的股票: {selected_codes}")
                    
                    # 计算当前总市值
                    total_value = self.cash
                    for code in list(self.positions.keys()):
                        if date in self.stock_data[code].index:
                            close_price = self.stock_data[code].loc[date, '收盘']
                            position_value = self.positions[code] * close_price
                            total_value += position_value
                    
                    # 检查持仓情况
                    if not self.positions:  # 如果没有持仓
                        # 买入选股池中的股票
                        per_stock_value = total_value / len(selected_codes)
                        for code in selected_codes:
                            if date in self.stock_data[code].index:
                                close_price = self.stock_data[code].loc[date, '收盘']
                                # 应用滑点
                                buy_price = self.apply_slippage(close_price, True)
                                # 计算手续费
                                commission = self.calculate_commission(per_stock_value, True)
                                # 实际可用资金
                                actual_value = per_stock_value - commission
                                # 计算可买入数量（向下取整到100股的倍数）
                                shares = int(actual_value / buy_price / 100) * 100
                                if shares > 0:  # 只有当可以买入至少100股时才执行
                                    self.positions[code] = shares  # 记录持仓数量
                                    self.cash -= (shares * buy_price + commission)  # 扣除买入股票的资金和手续费
                                    self.trade_history.append({
                                        'date': date,
                                        'code': code,
                                        'action': 'buy',
                                        'price': buy_price,
                                        'shares': shares,
                                        'value': shares * buy_price,
                                        'commission': commission
                                    })
                                    print(f"日期 {date.strftime('%Y-%m-%d')} 建仓: {code}, 价格: {buy_price:.2f}, 数量: {shares}, 手续费: {commission:.2f}")
                                else:
                                    print(f"日期 {date.strftime('%Y-%m-%d')} 资金不足，无法买入 {code}，需要资金: {buy_price * 100:.2f}，可用资金: {actual_value:.2f}")
                    else:  # 如果有持仓
                        # 检查持仓是否和选股池一致
                        current_holdings = set(self.positions.keys())
                        selected_set = set(selected_codes)
                        
                        if current_holdings != selected_set:  # 如果不一致
                            # 卖出不在选股池中的股票
                            for code in list(self.positions.keys()):
                                if code not in selected_codes:
                                    if date in self.stock_data[code].index:
                                        close_price = self.stock_data[code].loc[date, '收盘']
                                        # 应用滑点
                                        sell_price = self.apply_slippage(close_price, False)
                                        position_value = self.positions[code] * sell_price
                                        # 计算手续费
                                        commission = self.calculate_commission(position_value, False)
                                        # 实际获得资金
                                        actual_value = position_value - commission
                                        self.cash += actual_value
                                        self.trade_history.append({
                                            'date': date,
                                            'code': code,
                                            'action': 'sell',
                                            'price': sell_price,
                                            'shares': self.positions[code],
                                            'value': actual_value,
                                            'commission': commission
                                        })
                                        print(f"日期 {date.strftime('%Y-%m-%d')} 平仓: {code}, 价格: {sell_price:.2f}, 数量: {self.positions[code]}, 手续费: {commission:.2f}")
                                        del self.positions[code]  # 删除持仓记录
                            
                            # 计算剩余资金
                            remaining_value = self.cash
                            for code, value in self.position_values.items():
                                remaining_value += value
                            
                            # 买入选股池中新增的股票
                            new_stocks = selected_set - current_holdings
                            if new_stocks:
                                per_stock_value = remaining_value / len(new_stocks)
                                for code in new_stocks:
                                    if date in self.stock_data[code].index:
                                        close_price = self.stock_data[code].loc[date, '收盘']
                                        # 应用滑点
                                        buy_price = self.apply_slippage(close_price, True)
                                        # 计算手续费
                                        commission = self.calculate_commission(per_stock_value, True)
                                        # 实际可用资金
                                        actual_value = per_stock_value - commission
                                        # 计算可买入数量（向下取整到100股的倍数）
                                        shares = int(actual_value / buy_price / 100) * 100
                                        if shares > 0:  # 只有当可以买入至少100股时才执行
                                            self.positions[code] = shares  # 记录持仓数量
                                            self.cash -= (shares * buy_price + commission)  # 扣除买入股票的资金和手续费
                                            self.trade_history.append({
                                                'date': date,
                                                'code': code,
                                                'action': 'buy',
                                                'price': buy_price,
                                                'shares': shares,
                                                'value': shares * buy_price,
                                                'commission': commission
                                            })
                                            print(f"日期 {date.strftime('%Y-%m-%d')} 建仓: {code}, 价格: {buy_price:.2f}, 数量: {shares}, 手续费: {commission:.2f}")
                                        else:
                                            print(f"日期 {date.strftime('%Y-%m-%d')} 资金不足，无法买入 {code}，需要资金: {buy_price * 100:.2f}，可用资金: {actual_value:.2f}")
            
            # 计算当日总市值
            current_total_value = self.cash
            print(f"日期: {date}, 现金: {self.cash:.2f}")
            
            # 根据持仓计算市值
            for code, shares in self.positions.items():
                if date in self.stock_data[code].index:
                    close_price = self.stock_data[code].loc[date, '收盘']
                    position_value = shares * close_price
                    current_total_value += position_value
                    print(f"股票: {code}, 持仓数量: {shares}, 价格: {close_price:.2f}, 市值: {position_value:.2f}")
                else:
                    # 如果是节假日，使用最近一个交易日的收盘价
                    last_trading_day = self.stock_data[code].index[self.stock_data[code].index < date][-1]
                    close_price = self.stock_data[code].loc[last_trading_day, '收盘']
                    position_value = shares * close_price
                    current_total_value += position_value
                    print(f"股票: {code}, 持仓数量: {shares}, 价格: {close_price:.2f} (使用{last_trading_day.strftime('%Y-%m-%d')}收盘价), 市值: {position_value:.2f}")
            print(f"当日总市值: {current_total_value:.2f}")
            
            # 计算收益率
            if i > 0:  # 不是第一天
                prev_total_value = self.portfolio_value[-1]  # 使用前一天的投资组合价值
                print(f"昨日总市值: {prev_total_value:.2f}")
                
                if prev_total_value > 0:
                    daily_return = (current_total_value / prev_total_value) - 1
                    print(f"收益率: {daily_return:.2%}")
                else:
                    print("警告：昨日总市值为0")
                    daily_return = 0
            else:  # 第一天
                daily_return = 0
                print("第一天，收益率为0")
            
            # 更新投资组合价值
            portfolio_value = current_total_value  # 直接使用当日计算的总市值
            
            self.portfolio_value.append(portfolio_value)
            self.returns.append(daily_return)
            print(f"投资组合价值: {portfolio_value:.2f}\n")
        
        # 检查是否有足够的收益率数据
        if len(self.returns) == 0:
            print("错误：没有计算出任何收益率，请检查数据是否正确")
        else:
            print(f"回测完成，共计算出 {len(self.returns)} 个交易日的收益率")
            
        self.returns = pd.Series(self.returns)
        
    def generate_report(self):
        """生成回测报告"""
        try:
            # 检查是否有足够的数据进行回测
            if len(self.returns) == 0:
                print("错误：没有足够的数据进行回测，请确保数据获取成功")
                return
                
            # 计算回测指标
            returns_series = pd.Series(self.returns)
            
            # 计算总收益率
            total_return = (self.portfolio_value[-1] / self.portfolio_value[0]) - 1
            
            # 计算年化收益率
            annual_return = (1 + total_return) ** (252 / len(self.returns)) - 1
            
            # 计算年化波动率
            volatility = returns_series.std() * np.sqrt(252)
            
            # 计算夏普比率（假设无风险利率为3%）
            risk_free_rate = 0.03
            sharpe_ratio = (annual_return - risk_free_rate) / volatility if volatility != 0 else 0
            
            # 计算最大回撤
            portfolio_series = pd.Series(self.portfolio_value)
            cummax = portfolio_series.cummax()
            drawdown = (portfolio_series - cummax) / cummax
            max_drawdown = drawdown.min() if not drawdown.empty else 0
            
            # 计算胜率
            win_trades = 0
            total_trades = 0
            for trade in self.trade_history:
                if trade['action'] == 'sell':
                    total_trades += 1
                    # 计算该笔交易的盈亏
                    buy_trades = [t for t in self.trade_history 
                                if t['code'] == trade['code'] 
                                and t['action'] == 'buy' 
                                and t['date'] < trade['date']]
                    if buy_trades:
                        last_buy = buy_trades[-1]
                        profit = (trade['price'] - last_buy['price']) * trade['shares']
                        if profit > 0:
                            win_trades += 1
            
            win_rate = win_trades / total_trades if total_trades > 0 else 0
            
            # 获取上证指数收益率数据
            current_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            sh_data = pd.read_csv(os.path.join(current_dir, 'data', 'SH', 'sh_returns.csv'))
            sh_data['日期'] = pd.to_datetime(sh_data['日期'])
            
            # 绘制回测结果
            plt.figure(figsize=(12, 6))
            
            # 创建日期索引
            date_index = pd.date_range(start=self.start_date, end=self.end_date)
            
            # 确保投资组合数据长度匹配
            if len(date_index) > len(self.portfolio_value):
                date_index = date_index[:len(self.portfolio_value)]
            elif len(date_index) < len(self.portfolio_value):
                self.portfolio_value = self.portfolio_value[:len(date_index)]
            
            # 计算相对于初始值的百分比变化
            initial_value = self.portfolio_value[0]
            portfolio_percentage = [(value / initial_value - 1) * 100 for value in self.portfolio_value]
            
            # 将投资组合数据转换为DataFrame，方便后续处理
            portfolio_df = pd.DataFrame({
                '日期': date_index,
                '收益率': portfolio_percentage
            })
            
            # 筛选出与上证指数日期匹配的数据
            portfolio_df = portfolio_df[portfolio_df['日期'].isin(sh_data['日期'])]
            
            # 计算超额收益
            excess_return = portfolio_df['收益率'].values - sh_data['累计收益率(%)'].values
            
            # 绘制投资组合收益率
            plt.plot(portfolio_df['日期'], portfolio_df['收益率'], label='投资组合', linewidth=2)
            
            # 绘制上证指数收益率
            plt.plot(sh_data['日期'], sh_data['累计收益率(%)'], label='上证指数', linestyle='--', linewidth=2, color='red')
            
            # 绘制超额收益
            plt.plot(portfolio_df['日期'], excess_return, label='超额收益', linestyle=':', linewidth=2, color='orange')
            
            plt.title('投资组合与上证指数收益率对比', fontsize=12)
            plt.xlabel('日期', fontsize=10)
            plt.ylabel('收益率（%）', fontsize=10)
            plt.grid(True)
            plt.legend()
            # 设置x轴日期格式
            plt.gcf().autofmt_xdate()  # 自动旋转日期标签
            plt.tight_layout()
            plt.savefig('backtest_plot.png', dpi=300, bbox_inches='tight')
            plt.close()
            
            # 绘制每日收益率柱状图
            plt.figure(figsize=(15, 6))
            
            # 创建日期索引
            date_index = pd.date_range(start=self.start_date, end=self.end_date)
            
            # 确保投资组合数据长度匹配
            if len(date_index) > len(self.portfolio_value):
                date_index = date_index[:len(self.portfolio_value)]
            elif len(date_index) < len(self.portfolio_value):
                self.portfolio_value = self.portfolio_value[:len(date_index)]
            
            # 计算每日收益率
            daily_returns = []
            for i in range(1, len(self.portfolio_value)):
                daily_return = (self.portfolio_value[i] / self.portfolio_value[i-1]) - 1
                daily_returns.append(daily_return)
            
            bars = plt.bar(date_index[1:], daily_returns)
            plt.title('每日收益率', fontsize=12)
            plt.xlabel('日期', fontsize=10)
            plt.ylabel('收益率', fontsize=10)
            plt.grid(True, axis='y')
            
            # 设置柱状图颜色
            for bar in bars:
                if bar.get_height() >= 0:
                    bar.set_color('red')
                else:
                    bar.set_color('green')
            
            # 设置x轴日期格式
            plt.gcf().autofmt_xdate()  # 自动旋转日期标签
            
            plt.tight_layout()
            plt.savefig('daily_returns.png', dpi=300, bbox_inches='tight')
            plt.close()
            
            # 生成交易历史表格
            trade_df = pd.DataFrame(self.trade_history)
            trade_table = ""
            if not trade_df.empty:
                trade_table = """
                <h3>交易历史</h3>
                <table>
                    <tr>
                        <th>日期</th>
                        <th>股票代码</th>
                        <th>操作</th>
                        <th>价格（元）</th>
                        <th>数量（股）</th>
                        <th>交易金额（元）</th>
                        <th>手续费（元）</th>
                        <th>盈亏（元）</th>
                    </tr>
                """
                
                # 按日期分组
                trade_df['date'] = pd.to_datetime(trade_df['date'])
                trade_df = trade_df.sort_values('date')
                
                current_date = None
                for _, row in trade_df.iterrows():
                    # 如果是新的日期，添加一个分隔行
                    if current_date != row['date']:
                        if current_date is not None:
                            trade_table += """
                            <tr style="background-color: #f5f5f5;">
                                <td colspan="8"></td>
                            </tr>
                            """
                        current_date = row['date']
                    
                    profit = ""
                    if row['action'] == 'sell':
                        buy_trades = trade_df[(trade_df['code'] == row['code']) & 
                                            (trade_df['action'] == 'buy') & 
                                            (trade_df['date'] < row['date'])]
                        if not buy_trades.empty:
                            last_buy = buy_trades.iloc[-1]
                            profit = (row['price'] - last_buy['price']) * row['shares']
                            profit = f"{profit:.2f}"
                    
                    trade_table += f"""
                    <tr>
                        <td>{row['date'].strftime('%Y-%m-%d')}</td>
                        <td>{row['code']}</td>
                        <td>{'买入' if row['action'] == 'buy' else '卖出'}</td>
                        <td>{row['price']:.2f}</td>
                        <td>{row['shares']}</td>
                        <td>{row['value']:.2f}</td>
                        <td>{row['commission']:.2f}</td>
                        <td>{profit}</td>
                    </tr>
                    """
                trade_table += "</table>"
            
            # 生成HTML报告
            report = f"""
            <html>
            <head>
                <title>回测报告</title>
                <meta charset="utf-8">
                <style>
                    body {{ font-family: "Microsoft YaHei", Arial, sans-serif; margin: 20px; }}
                    table {{ border-collapse: collapse; width: 100%; margin-bottom: 20px; }}
                    th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                    th {{ background-color: #f2f2f2; }}
                    .positive {{ color: #d81e06; }}
                    .negative {{ color: #00a800; }}
                </style>
            </head>
            <body>
                <h1>回测报告</h1>
                <h2>回测期间：{self.start_date.strftime('%Y-%m-%d')} 至 {self.end_date.strftime('%Y-%m-%d')}</h2>
                
                <h3>回测参数</h3>
                <table>
                    <tr>
                        <th>参数</th>
                        <th>数值</th>
                    </tr>
                    <tr>
                        <td>调仓周期</td>
                        <td>{self.rebalance_period} 个交易日</td>
                    </tr>
                    <tr>
                        <td>每次选股数量</td>
                        <td>{self.top_n} 只</td>
                    </tr>
                    <tr>
                        <td>初始资金</td>
                        <td>50,000 元</td>
                    </tr>
                </table>
                
                <h3>回测结果</h3>
                <table>
                    <tr>
                        <th>指标</th>
                        <th>数值</th>
                    </tr>
                    <tr>
                        <td>总收益率</td>
                        <td class="{('positive' if total_return > 0 else 'negative')}">{total_return:+.2%}</td>
                    </tr>
                    <tr>
                        <td>年化收益率</td>
                        <td class="{('positive' if annual_return > 0 else 'negative')}">{annual_return:+.2%}</td>
                    </tr>
                    <tr>
                        <td>年化波动率</td>
                        <td>{volatility:.2%}</td>
                    </tr>
                    <tr>
                        <td>夏普比率</td>
                        <td class="{('positive' if sharpe_ratio > 0 else 'negative')}">{sharpe_ratio:+.2f}</td>
                    </tr>
                    <tr>
                        <td>最大回撤</td>
                        <td class="negative">{max_drawdown:.2%}</td>
                    </tr>
                    <tr>
                        <td>胜率</td>
                        <td>{win_rate:.2%}</td>
                    </tr>
                    <tr>
                        <td>期末总资产</td>
                        <td>{self.portfolio_value[-1]:,.2f} 元</td>
                    </tr>
                    <tr>
                        <td>期末现金</td>
                        <td>{self.cash:,.2f} 元</td>
                    </tr>
                    <tr>
                        <td>期末股票市值</td>
                        <td>{self.portfolio_value[-1] - self.cash:,.2f} 元</td>
                    </tr>
                </table>
                
                <h3>投资组合价值变化</h3>
                <img src="backtest_plot.png" alt="投资组合价值变化图" style="max-width: 100%;">
                
                <h3>每日收益率</h3>
                <img src="daily_returns.png" alt="每日收益率图" style="max-width: 100%;">
                
                {trade_table}
            </body>
            </html>
            """
            
            with open('backtest_report.html', 'w', encoding='utf-8') as f:
                f.write(report)
                
        except Exception as e:
            print(f"生成报告时出错: {str(e)}")
            raise 