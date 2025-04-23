import pandas as pd
import numpy as np
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib import font_manager

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号

class Backtest:
    def __init__(self, stock_data, start_date, end_date, factor_model=None, rebalance_period=20, top_n=3):
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
        
    def run_backtest(self):
        """运行回测"""
        # 初始化投资组合
        portfolio_value = 50000  # 初始资金5万
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
                        available_stocks[code] = data
                
                if available_stocks:
                    # 使用因子模型选股
                    self.factor_model.calculate_technical_factors(available_stocks)
                    self.factor_model.calculate_final_score()
                    selected_stocks = self.factor_model.select_top_stocks(self.top_n)
                    
                    # 记录选股结果
                    selected_codes = [code for code, _ in selected_stocks]
                    print(f"日期 {date.strftime('%Y-%m-%d')} 选出的股票: {selected_codes}")
                    
                    # 计算当前总市值
                    total_value = portfolio_value
                    for code in self.positions:
                        if date in self.stock_data[code].index:
                            close_price = self.stock_data[code].loc[date, '收盘']
                            position_value = self.positions[code] * close_price
                            total_value += position_value
                    
                    # 平掉不在选中列表中的股票
                    for code in list(self.positions.keys()):
                        if code not in selected_codes:
                            # 平仓
                            if date in self.stock_data[code].index:
                                close_price = self.stock_data[code].loc[date, '收盘']
                                position_value = self.positions[code] * close_price
                                total_value += position_value
                                self.trade_history.append({
                                    'date': date,
                                    'code': code,
                                    'action': 'sell',
                                    'price': close_price,
                                    'shares': self.positions[code],
                                    'value': position_value
                                })
                                print(f"日期 {date.strftime('%Y-%m-%d')} 平仓: {code}, 价格: {close_price}, 数量: {self.positions[code]}")
                                del self.positions[code]
                                del self.position_values[code]
                    
                    # 计算每只股票的投资金额
                    if selected_codes:
                        per_stock_value = total_value / len(selected_codes)
                        
                        # 买入新选中的股票
                        for code in selected_codes:
                            if code not in self.positions and date in self.stock_data[code].index:
                                close_price = self.stock_data[code].loc[date, '收盘']
                                shares = per_stock_value / close_price
                                self.positions[code] = shares
                                self.position_values[code] = per_stock_value
                                self.trade_history.append({
                                    'date': date,
                                    'code': code,
                                    'action': 'buy',
                                    'price': close_price,
                                    'shares': shares,
                                    'value': per_stock_value
                                })
                                print(f"日期 {date.strftime('%Y-%m-%d')} 建仓: {code}, 价格: {close_price}, 数量: {shares}")
            
            # 计算当日收益率
            daily_return = 0
            if self.positions:
                # 计算持仓股票的当日收益率
                position_returns = []
                position_weights = []
                total_value = portfolio_value
                
                # 计算总市值和持仓权重
                for code in self.positions:
                    if date in self.stock_data[code].index:
                        close_price = self.stock_data[code].loc[date, '收盘']
                        position_value = self.positions[code] * close_price
                        total_value += position_value
                        self.position_values[code] = position_value
                
                # 计算加权收益率
                for code in self.positions:
                    if date in self.stock_data[code].index and (date - pd.Timedelta(days=1)) in self.stock_data[code].index:
                        stock_return = self.stock_data[code].loc[date, '收盘'] / self.stock_data[code].loc[date - pd.Timedelta(days=1), '收盘'] - 1
                        weight = self.position_values[code] / total_value
                        position_returns.append(stock_return)
                        position_weights.append(weight)
                
                if position_returns:
                    # 计算投资组合加权收益率
                    daily_return = np.sum(np.array(position_returns) * np.array(position_weights))
            
            # 更新投资组合价值
            portfolio_value *= (1 + daily_return)
            self.portfolio_value.append(portfolio_value)
            self.returns.append(daily_return)
        
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
            
            # 计算年化收益率
            annual_return = (1 + returns_series.mean()) ** 252 - 1
            
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
            win_rate = (returns_series > 0).mean()
            
            # 计算总收益率
            total_return = (self.portfolio_value[-1] / self.portfolio_value[0]) - 1
            
            # 绘制回测结果
            plt.figure(figsize=(12, 6))
            # 创建日期索引
            date_index = pd.date_range(start=self.start_date, end=self.end_date)
            # 确保数据长度匹配
            if len(date_index) > len(self.portfolio_value):
                date_index = date_index[:len(self.portfolio_value)]
            elif len(date_index) < len(self.portfolio_value):
                self.portfolio_value = self.portfolio_value[:len(date_index)]
            
            plt.plot(date_index, self.portfolio_value)
            plt.title('投资组合价值变化', fontsize=12)
            plt.xlabel('日期', fontsize=10)
            plt.ylabel('投资组合价值（元）', fontsize=10)
            plt.grid(True)
            # 设置x轴日期格式
            plt.gcf().autofmt_xdate()  # 自动旋转日期标签
            plt.tight_layout()
            plt.savefig('backtest_plot.png', dpi=300, bbox_inches='tight')
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
                    </tr>
                """
                for _, row in trade_df.iterrows():
                    trade_table += f"""
                    <tr>
                        <td>{row['date'].strftime('%Y-%m-%d')}</td>
                        <td>{row['code']}</td>
                        <td>{'买入' if row['action'] == 'buy' else '卖出'}</td>
                        <td>{row['price']:.2f}</td>
                        <td>{row['shares']:.2f}</td>
                        <td>{row['value']:.2f}</td>
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
                </table>
                
                <h3>投资组合价值变化</h3>
                <img src="backtest_plot.png" alt="投资组合价值变化图" style="max-width: 100%;">
                
                {trade_table}
            </body>
            </html>
            """
            
            with open('backtest_report.html', 'w', encoding='utf-8') as f:
                f.write(report)
                
        except Exception as e:
            print(f"生成报告时出错: {str(e)}")
            raise 