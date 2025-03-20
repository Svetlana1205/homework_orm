import sqlalchemy as sq
import json
import os
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

Base = declarative_base()


class Publisher(Base):
    __tablename__ = 'publisher'

    id = sq.Column(sq.Integer, primary_key=True)
    name = sq.Column(sq.String(length=50), unique=True, nullable=False)

    books = relationship("Book", back_populates="publisher")


class Book(Base):
    __tablename__ = 'book'

    id = sq.Column(sq.Integer, primary_key=True)
    title = sq.Column(sq.String(length=50), unique=True, nullable=False)
    id_publisher = sq.Column(sq.Integer, sq.ForeignKey('publisher.id'), nullable=False)

    publisher = relationship("Publisher", back_populates="books")
    stocks = relationship("Stock", back_populates="book")


class Shop(Base):
    __tablename__ = 'shop'

    id = sq.Column(sq.Integer, primary_key=True)
    name = sq.Column(sq.String(length=50), unique=True, nullable=False)

    stocks = relationship("Stock", back_populates="shop")


class Stock(Base):
    __tablename__ = 'stock'

    id = sq.Column(sq.Integer, primary_key=True)
    id_book = sq.Column(sq.Integer, sq.ForeignKey('book.id'), nullable=False)
    id_shop = sq.Column(sq.Integer, sq.ForeignKey('shop.id'), nullable=False)
    count = sq.Column(sq.Integer, nullable=False)

    book = relationship("Book", back_populates="stocks")
    shop = relationship("Shop", back_populates="stocks")
    sales = relationship("Sale", back_populates="stock")


class Sale(Base):
    __tablename__ = 'sale'

    id = sq.Column(sq.Integer, primary_key=True)
    price = sq.Column(sq.Float, nullable=False)
    date_sale = sq.Column(sq.Date, nullable=False)
    id_stock = sq.Column(sq.Integer, sq.ForeignKey('stock.id'), nullable=False)
    count = sq.Column(sq.Integer, nullable=False)

    stock = relationship("Stock", back_populates="sales")


def create_tables(engine):
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)


# Подключение к базе данных
DSN = '...'
engine = sq.create_engine(DSN)

create_tables(engine)

Session = sessionmaker(bind=engine)
session = Session()

# Загрузка данных из JSON
file_path = os.path.join('tests_data.json')

with open(file_path, "r", encoding="utf-8") as file:
    data = json.load(file)

# Словарь для сопоставления моделей
model_map = {
    "publisher": Publisher,
    "book": Book,
    "shop": Shop,
    "stock": Stock,
    "sale": Sale
}

# Добавление данных в БД
for entry in data:
    model_name = entry['model']
    model_class = model_map.get(model_name)

    if model_class:
        entry_data = entry['fields']
        entry_data['id'] = entry.pop('pk')
        session.add(model_class(**entry_data))

# Сохраняем изменения в БД
session.commit()

# Получаем данные от пользователя
publisher_input = input("Введите имя или ID издателя: ")

# Определяем, введено имя или ID
if publisher_input.isdigit():
    publisher = session.query(Publisher).filter(Publisher.id == int(publisher_input)).first()
else:
    publisher = session.query(Publisher).filter(Publisher.name == publisher_input).first()

# Если издатель найден
if publisher:
    print(f"\nФакты покупок книг издателя: {publisher.name}\n")

    query = (
        session.query(Book.title, Shop.name, Sale.price, Sale.date_sale)
        .join(Stock, Stock.id_book == Book.id)
        .join(Shop, Shop.id == Stock.id_shop)
        .join(Sale, Sale.id_stock == Stock.id)
        .filter(Book.id_publisher == publisher.id)
        .order_by(Sale.date_sale.desc())
    )

    results = query.all()

    if results:
        for book_title, shop_name, price, date_sale in results:
            print(f"{book_title} | {shop_name:<12} | {price:<5} | {date_sale.strftime('%d-%m-%Y')}")
    else:
        print("У этого издателя нет продаж.")

else:
    print("Издатель не найден!")

session.close()
