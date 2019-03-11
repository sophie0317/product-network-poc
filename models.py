from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Time, Float, Sequence, ForeignKey
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.exc import IntegrityError

engine = create_engine('postgresql://phil:@127.0.0.1:5432/product')

Session = scoped_session(sessionmaker(bind=engine))

session = Session()

Base = declarative_base(engine)

class Manager:
    @classmethod
    def filter(cls, **kwargs):
        pass

    @classmethod
    def all(cls):
        return session.query(cls).all()

    @classmethod
    def get(cls, **kwargs):
        return session.query(cls).filter_by(**kwargs).first()

    @classmethod
    def create(cls, **kwargs):
        instance = session.query(cls).filter_by(**kwargs).first()
        if not instance:
            try:
                cls._create(**kwargs)
            except IntegrityError:
                session.rollback()
                return False
            else:
                return True

    @classmethod
    def _create(cls, **kwargs):
        session.add(cls(**kwargs))
        session.commit()


class Transaction(Base, Manager):
    __tablename__ = 'transactions'
    id = Column(String(32), primary_key=True)
    time = Column(Time)
    time_phase = Column(String(10))
    location = Column(String(10))
    branch_id = Column(Integer)
    location_type = Column(String(10))

    def __str__(self):
        return '<[{}] {} {}>'.format(self.id, self.time, self.location_type)
    
    def __repl__(self):
        return '<[{}] {} {}>'.format(self.id, self.time, self.location_type)


class Item(Base, Manager):
    __tablename__ = 'items'
    id = Column(Integer, Sequence('items_id_seq'), primary_key=True)
    type = Column(String(30))
    subtype = Column(String(30))
    name = Column(String(100))
    price = Column(Integer)
    objects = Manager()

    def __str__(self):
        return '<[{}] {} {}>'.format(self.id, self.name, self.price)

    def __repl__(self):
        return '<[{}] {} {}>'.format(self.id, self.name, self.price)


class Transaction_Item(Base, Manager):
    __tablename__ = 'transaction_item'
    transaction_id = Column(String(32), ForeignKey(
        'transactions.id'), primary_key=True)
    item_id = Column(Integer, ForeignKey('items.id'), primary_key=True)
    times = Column(Integer)
    transaction_amount = Column(Float)

    def __str__(self):
        return '<[{}] {}>'.format(self.transaction_id, self.item_id)

    def __repl__(self):
        return '<[{}] {}>'.format(self.transaction_id, self.item_id)
