import pandas as pd
import numpy
import profile
from models import engine

dtype = {
    '交易id': numpy.str,
    '資料日期': numpy.str,
    '資料時間': numpy.str,
    '餐別帶': numpy.str,
    '縣市別': numpy.str,
    '店舖代號': numpy.uint32,
    '主商圈': numpy.str,
    '品號-品名稱': numpy.str,
    '群號-群名稱': numpy.str,
    '單品名稱': numpy.str,
    '銷售數量': numpy.uint16,
    '銷售單價': numpy.float,
    '交易金額': numpy.float
}

USE_COLUMNS = ['交易id', '資料日期', '資料時間', '餐別帶', '縣市別', '店舖代號', '主商圈', '品號-品名稱',
               '群號-群名稱', '單品名稱', '銷售數量', '銷售單價', '交易金額']
PARSE_DATES = {
    '資料日期與時間': [
        '資料日期',
        '資料時間'
    ]
}

def read_from_csv(limit_records, *kwargs):
    records = pd.read_csv('customer_data(utf-8).csv',
                                 index_col=1,
                                 nrows=limit_records,
                                 usecols=USE_COLUMNS,
                                 dtype=dtype,
                                 parse_dates=PARSE_DATES,
                                 )
    return records[records['縣市別'] == '台中市']

def read_from_database(query):
    records = pd.read_sql_query(query, con=engine)
    return records

query = """
SELECT * FROM transaction_item
JOIN transactions ON transaction_item.transaction_id = transactions.id
JOIN items ON items.id = transaction_item.item_id
WHERE transactions.location = '台中市';
"""

profiler = profile.Profile()
profiler.runcall(read_from_csv, 1000)
profiler.print_stats()

profiler.runcall(read_from_database, query)
profiler.print_stats()
