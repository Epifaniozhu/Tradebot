import sqlite3
import pandas as pd
from datetime import datetime

def check_data_quality():
    # 连接数据库
    conn = sqlite3.connect('data/market_data.db')
    
    # 检查时间范围和价格
    print('\n=== 时间范围和价格检查 ===')
    query = '''
    SELECT 
        symbol,
        MIN(timestamp) as start_time,
        MAX(timestamp) as end_time,
        MIN(close) as min_price,
        MAX(close) as max_price,
        AVG(volume) as avg_volume,
        AVG(quote_volume) as avg_quote_volume
    FROM kline_data 
    GROUP BY symbol
    '''
    
    df = pd.read_sql(query, conn)
    
    # 转换时间戳
    df['start_time'] = pd.to_datetime(df['start_time'], unit='ms')
    df['end_time'] = pd.to_datetime(df['end_time'], unit='ms')
    
    # 打印结果
    for _, row in df.iterrows():
        print(f"\n{row['symbol']}:")
        print(f"  时间范围: {row['start_time']} 到 {row['end_time']}")
        print(f"  价格范围: {row['min_price']:.4f} - {row['max_price']:.4f}")
        print(f"  平均成交量: {row['avg_volume']:.2f}")
        print(f"  平均USDT成交量: {row['avg_quote_volume']/1000000:.2f}M")
    
    conn.close()

if __name__ == "__main__":
    check_data_quality() 