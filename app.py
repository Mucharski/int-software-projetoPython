from types import SimpleNamespace

from confluent_kafka import Consumer
from sqlalchemy import create_engine, Integer, Column, Boolean, String, Numeric, Date
from sqlalchemy.orm import declarative_base
import json
from sqlalchemy import insert
from dateutil.parser import parse

engine = create_engine('sqlite:///databasePython.db', echo=True)
Base = declarative_base()


class RestaurantSchedules(Base):
    __tablename__ = 'RestaurantConfirmedSchedules'
    id=Column(Integer, primary_key=True)
    name=Column('name', String(255))
    scheduledDate=Column('date', Date)
    confirmed=Column('confirmed', Boolean)


Base.metadata.create_all(engine)

print('Hearing Messages...')
c = Consumer({'bootstrap.servers': 'localhost:9092', 'group.id': 'python-consumer', 'auto.offset.reset': 'earliest'})
c.subscribe(['schedules'])
while True:
    msg = c.poll(1.0)  # timeout
    if msg is None:
        continue
    if msg.error():
        print('Error: {}'.format(msg.error()))
        continue
    data = msg.value().decode('utf-8')
    deserializedObject = json.loads(data, object_hook=lambda d: SimpleNamespace(**d))
    stmt = (
        insert(RestaurantSchedules).
        values(name=deserializedObject.restaurantName, date=parse(deserializedObject.scheduledTo), confirmed=True)
    )
    with engine.connect() as conn:
        result = conn.execute(stmt)
        conn.commit()
    print(deserializedObject)
c.close()
