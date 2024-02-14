import os

class DatabaseWriter:
    def __init__(self, json_file_path):
        self.json_file_path = json_file_path
        self.Session = sessionmaker(bind=engine)

    def find_json_file(self):
        # Search for the JSON file in the current directory and its parent directories
        current_dir = os.path.dirname(os.path.abspath(__file__))
        parent_dir = os.path.dirname(current_dir)
        dirs_to_search = [current_dir, parent_dir]

        for directory in dirs_to_search:
            potential_path = os.path.join(directory, self.json_file_path)
            if os.path.isfile(potential_path):
                self.json_file_path = potential_path
                return True

        # Search the entire filesystem if the file is not found in nearby directories
        for root, dirs, files in os.walk('/'):
            if self.json_file_path in files:
                self.json_file_path = os.path.join(root, self.json_file_path)
                return True

        return False

    def write_to_database(self):
        if not self.find_json_file():
            print(f"File {self.json_file_path} not found.")
            return

        print(f"Found file: {self.json_file_path}")

        with open(self.json_file_path, 'r', encoding='utf-8-sig') as file:
            data = json.load(file)

        session = self.Session()
        for model_name, model_info in mappings.items():
            model_class = globals()[model_name]
            json_key = model_info['json_key']
            for chunk in data[json_key]:
                model_data = {model_field: chunk.get(json_field, None) for model_field, json_field in
                              model_info['mapping'].items()}
                model_instance = model_class(**model_data)
                session.add(model_instance)
        session.commit()
        session.close()


# from sqlalchemy import create_engine, Column, Integer, Numeric, String, TIMESTAMP, Boolean
# from sqlalchemy.orm import declarative_base, session
# from sqlalchemy import Column, Integer, Numeric, String
# from sqlalchemy.ext.declarative import declarative_base
# from sqlalchemy.schema import CreateSchema
import json




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

mappings = {
    'AccruedCommissions': {
        'json_key': 'НачисленныеКомиссии',
        'mapping': {
            'period': 'Период',
            'date': 'Дата',
            'number': 'Номер',
            'type_': 'Вид',
            'tariff': 'Тариф',
            'amount': 'Сумма',
            'registrar': 'Регистратор',
        }
    },
    'RepoDocuments': {
        'json_key': 'ДокуметыРЕПО',
        'mapping': {
            'passport': 'Паспорт',
            'order_info': 'Заказ',
            'document': 'Документ',
            'deal_type': 'ВидСделки',
            'number': 'Номер',
            'date': 'Дата',
            'currency': 'Валюта',
            'market': 'Рынок',
            'isin': 'ISIN',
            'ticker': 'Тикер',
            'financial_instrument': 'ФинансовыйИнструмент',
            'settlement_date': 'ДатаРасчетов',
            'opening_date': 'ДатаОткрытия',
            'closing_date': 'ДатаЗакрытия',
            'order_date': 'ЗаказДата',
            'order_number': 'ЗаказНомер',
            'opening_price': 'ЦенаОткрытия',
            'closing_price': 'ЦенаЗакрытия',
            'quantity': 'Количество',
            'interest_rate': 'Доходность',
            'accrued_interest': 'НКД',
            'opening_amount': 'СуммаОткрытия',
            'closing_amount': 'СуммаЗакрытия',
            'order_opening_number': 'НомерЗаявкиОткрытия',
            'kase_order_number': 'НомерЗаявкиНаКАСЕ',
            'market_type': 'ВидРынка',
            'exchange_type': 'ТипРынка',
            'execution_status': 'СтатусИсполнения',
            'transaction_method': 'МетодПроведенияСделок',
        }
    },
    'TradeDocuments': {
        'json_key': 'ДокументыПокупкаПродажа',
        'mapping': {
            'passport': 'Паспорт',
            'order_info': 'Заказ',
            'document': 'Документ',
            'deal_type': 'ВидСделки',
            'number': 'Номер',
            'date': 'Дата',
            'currency': 'Валюта',
            'market': 'Рынок',
            'isin': 'ISIN',
            'ticker': 'Тикер',
            'financial_instrument': 'ФинансовыйИнструмент',
            'settlement_date': 'ДатаРасчетов',
            'opening_date': 'ДатаОткрытия',
            'closing_date': 'ДатаЗакрытия',
            'order_date': 'ЗаказДата',
            'order_number': 'ЗаказНомер',
            'price': 'Цена',
            'quantity': 'Количество',
            'interest_rate': 'Доходность',
            'accrued_interest': 'НКД',
            'amount': 'Сумма',
            'closing_amount': 'СуммаЗакрытия',
            'opening_order_number': 'НомерЗаявкиОткрытия',
            'kase_order_number': 'НомерЗаявкиНаКАСЕ',
            'market_type': 'ВидРынка',
            'exchange_type': 'ТипРынка',
            'swap': 'SWOP',
            'currency_market': 'ВалютныйРынок',
            'execution_status': 'СтатусИсполнения',
            'transaction_method': 'МетодПроведенияСделок',
        }
    },
    'FinancialInstrumentsMovement': {
        'json_key': 'ДвижениеФИ',
        'mapping': {
            'financial_instrument': 'ФинансовыйИнструмент',
            'account_number': 'ЛицевойСчет',
            'opening_balance': 'КоличествоНачальныйОстаток',
            'closing_balance': 'КоличествоКонечныйОстаток',
            'turnover': 'КоличествоОборот',
            'amount_in': 'КоличествоПриход',
            'amount_out': 'КоличествоРасход',
            'issuer': 'Эмитент',
            'currency_of_issue': 'ВалютаВыпуска',
            'type_of_security': 'ВидЦБ',
            'fair_price': 'СправедливаяЦена',
            'ISIN': 'ИСИН',
            'sum_in_kzt': 'СуммаKZT',
            'ticker': 'Тикер',
        }
    },
    'MoneyMovement': {
        'json_key': 'ДвижениеДенег',
        'mapping': {
            'bank_account': 'БанковскийСчет',
            'currency': 'Валюта',
            'account_number': 'НомерСчета',
            'closing_balance': 'СуммаКонечныйОстаток',
            'opening_balance': 'СуммаНачальныйОстаток',
            'money_in': 'СуммаПриход',
            'money_out': 'СуммаРасход',
        }
    }
}



if __name__ == "__main__":
    writer = DatabaseWriter('../1C_827496_20231112.json')
    writer.write_to_database()
