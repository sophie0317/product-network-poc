import pandas as pd
import numpy
from timeit import default_timer as timer
from models import Transaction, Item, Transaction_Item

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


def extract_transaction(index, row):
    return (
        ('id', index),
        ('time', row['資料日期與時間']),
        ('time_phase', row['餐別帶']),
        ('branch_id', row['店舖代號']),
        ('location_type', row['主商圈']),
        ('location', row['縣市別'])
    )


def extract_item(row):
    return (
        ('type', row['品號-品名稱']),
        ('subtype', row['群號-群名稱']),
        ('name', row['單品名稱']),
        ('price', row['銷售單價'])
    )


def extract_transaction_item(index, row):
    return (
        ('transaction_id', index),
        ('item_name', row['單品名稱']),
        ('times', row['銷售數量']),
        ('transaction_amount', row['交易金額'])
    )


def write(df):
    items_added = 0
    transactions_added = 0
    transaction_record_added = 0
    item_set = set()
    transaction_set = set()
    transaction_record_set = set()

    for index, row in df.iterrows():
        item_set.add(extract_item(row))
        transaction_set.add(extract_transaction(index, row))
        transaction_record_set.add(extract_transaction_item(index, row))

    for item in item_set:
        is_added = Item.create(**dict(item))
        if is_added:
            items_added += 1

    for transaction in transaction_set:
        is_added = Transaction.create(**dict(transaction))
        if is_added:
            transactions_added += 1

    for record in transaction_record_set:
        record = dict(record)
        item = Item.get(name=record['item_name'])
        if item is None:
            continue
        record['item_id'] = item.id
        del record['item_name']
        is_added = Transaction_Item.create(**record)
        if is_added:
            transaction_record_added += 1
    print('\rItems added: {}\nTransactions added: {}\nRecords added: {}\n'.format(
        items_added, transactions_added, transaction_record_added), flush=True)


chunk_iterator = pd.read_csv('customer_data(utf-8).csv',
                             index_col=1,
                             chunksize=100000,
                             usecols=USE_COLUMNS,
                             dtype=dtype,
                             parse_dates=PARSE_DATES,
                             )

start = timer()
for chunk in chunk_iterator:
    chunk = chunk.dropna()
    write(chunk)
end = timer()
print('Total time elapsed: {}'.format(end - start))
