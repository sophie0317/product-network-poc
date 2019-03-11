from connect import connect

config = {
    'host': '127.0.0.1',
    'port': '5432',
    'dbname': 'product_test',
    'user': 'phil'
}

create_transaction = """
CREATE TABLE Transactions(
    Id VARCHAR (32) PRIMARY KEY,
    Time timestamp NOT NULL,
    Time_Phase VARCHAR (10) NOT NULL,
    Location VARCHAR (10) NOT NULL,
    Branch_ID int NOT NULL,
    Location_Type VARCHAR (10) NOT NULL
);
"""

create_item = """
CREATE TABLE Items(
    Id  SERIAL PRIMARY KEY,
    Type VARCHAR(30) NOT NULL,
    Subtype VARCHAR(30) NOT NULL,
    Name VARCHAR(100) NOT NULL,
    Price float8 NOT NULL,
    UNIQUE (Type, Subtype, Name)
);
"""

create_transaction_item = """
CREATE TABLE Transaction_Item(
    Transaction_Id varchar(32) REFERENCES Transactions(Id),
    Item_Id integer REFERENCES Items(Id),
    Times integer NOT NULL,
    Transaction_Amount float8 NOT NULL,
    CONSTRAINT Transaction_Item_Id PRIMARY KEY (Transaction_Id, Item_Id)
);
"""

commands = [
    create_item,
    create_transaction,
    create_transaction_item
]

conn = None

try:
    conn = connect(**config)
    cursor = conn.cursor()
    for command in commands:
        cursor.execute(command)
    cursor.close()
    conn.commit()
except Exception as err:
    print(err)
finally:
    if conn is not None:
        conn.close()
