

from sqlalchemy import create_engine, Column, Integer, Numeric, String, TIMESTAMP, Boolean
from sqlalchemy.orm import declarative_base, sessionmaker

Base = declarative_base()


class MoneyMovement(Base):
    __tablename__ = 'money_movement'
    # __table_args__ = {'schema': 'json_alchemy'}
    id = Column(Integer, primary_key=True, autoincrement=True)
    bank_account = Column(String)
    currency = Column(String)
    account_number = Column(String)
    closing_balance = Column(Numeric)
    opening_balance = Column(Numeric)
    money_in = Column(Numeric)
    money_out = Column(Numeric)


class FinancialInstrumentsMovement(Base):
    __tablename__ = 'financial_instruments_movement'
    # __table_args__ = {'schema': 'json_alchemy'}
    id = Column(Integer, primary_key=True, autoincrement=True)
    financial_instrument = Column(String)
    account_number = Column(String)
    opening_balance = Column(Integer)
    closing_balance = Column(Integer)
    turnover = Column(Integer)
    amount_in = Column(Integer)
    amount_out = Column(Integer)
    issuer = Column(String)
    currency_of_issue = Column(String)
    type_of_security = Column(String)
    fair_price = Column(Numeric)
    ISIN = Column(String)
    sum_in_kzt = Column(Numeric)
    ticker = Column(String)


class TradeDocuments(Base):
    __tablename__ = 'trade_documents'
    # __table_args__ = {'schema': 'json_alchemy'}
    id = Column(Integer, primary_key=True, autoincrement=True)
    passport = Column(String)
    order_info = Column(String)
    document = Column(String)
    deal_type = Column(String)
    number = Column(String)
    date = Column(TIMESTAMP)
    currency = Column(String)
    market = Column(String)
    isin = Column(String)
    ticker = Column(String)
    financial_instrument = Column(String)
    settlement_date = Column(TIMESTAMP)
    opening_date = Column(TIMESTAMP, nullable=True)
    closing_date = Column(TIMESTAMP, nullable=True)
    order_date = Column(TIMESTAMP)
    order_number = Column(String)
    price = Column(Numeric)
    quantity = Column(Integer)
    interest_rate = Column(Numeric)
    accrued_interest = Column(Numeric)
    amount = Column(Numeric)
    closing_amount = Column(Numeric)
    opening_order_number = Column(String)
    kase_order_number = Column(String)
    market_type = Column(String)
    exchange_type = Column(String)
    swap = Column(Boolean)
    currency_market = Column(Boolean)
    execution_status = Column(String)
    transaction_method = Column(String)


class RepoDocuments(Base):
    __tablename__ = 'repo_documents'
    # __table_args__ = {'schema': 'json_alchemy'}
    id = Column(Integer, primary_key=True, autoincrement=True)
    passport = Column(String)
    order_info = Column(String)
    document = Column(String)
    deal_type = Column(String)
    number = Column(String)
    date = Column(TIMESTAMP)
    currency = Column(String)
    market = Column(String)
    isin = Column(String)
    ticker = Column(String)
    financial_instrument = Column(String)
    settlement_date = Column(TIMESTAMP)
    opening_date = Column(TIMESTAMP, nullable=True)
    closing_date = Column(TIMESTAMP, nullable=True)
    order_date = Column(TIMESTAMP)
    order_number = Column(String)
    opening_price = Column(Numeric)
    closing_price = Column(Numeric)
    quantity = Column(Integer)
    interest_rate = Column(Numeric)
    accrued_interest = Column(Numeric)
    opening_amount = Column(Numeric)
    closing_amount = Column(Numeric)
    order_opening_number = Column(String)
    kase_order_number = Column(String)
    market_type = Column(String)
    exchange_type = Column(String)
    execution_status = Column(String)
    transaction_method = Column(String)


class AccruedCommissions(Base):
    __tablename__ = 'accrued_commissions'
    # __table_args__ = {'schema': 'json_alchemy'}
    id = Column(Integer, primary_key=True, autoincrement=True)
    period = Column(TIMESTAMP)
    date = Column(TIMESTAMP)
    number = Column(String)
    type_ = Column(String)
    tariff = Column(String)
    amount = Column(Numeric)
    registrar = Column(String)


engine = create_engine('postgresql://postgres:Muzzy_PG24@91.149.241.3/postgres')

# Создание sessionmaker
Session = sessionmaker(bind=engine)

# Создание сессии
session = Session()

# Создание схемы, если она не существует
# schema_name = 'json_alchemy'
# result = session.execute('SELECT schema_name FROM information_schema.schemata WHERE schema_name =json_alchemy')
# exists = result.first() is not None
# if not exists:
#     session.execute(CreateSchema(schema_name))
# # Применение изменений в базу данных
# session.commit()
# Создание таблиц, если они не существуют
Base.metadata.create_all(engine)
session.close()
#
# # Загрузка данных из файла
# with open('./inbox/test.json', 'r', encoding='utf-8') as file:
#     data = json.load(file)
#
# # Перебор данных и заполнение таблицы
# for item in data["НачисленныеКомиссии"]:  # Убедитесь, что ключ соответствует структуре вашего JSON файла
#     new_commission = AccruedCommissions(
#         period=item['Период'],
#         date=item['Дата'],
#         number=item['Номер'],
#         type_=item['Вид'],
#         tariff=item['Тариф'],
#         amount=item['Сумма'],
#         registrar=item['Регистратор']
#     )
#     session.add(new_commission)
#
# # Сохранение изменений в базе данных
# session.commit()
#
# # Закрытие сессии
# session.close()
