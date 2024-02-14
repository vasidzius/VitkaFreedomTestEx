from sqlalchemy.orm import sessionmaker
from alchemy import engine  # Убедитесь, что engine правильно импортирован из вашего файла alchemy.py
from alchemy import AccruedCommissions, RepoDocuments, TradeDocuments, FinancialInstrumentsMovement, MoneyMovement  # Импорт модели
from mappings import mappings
import json

Session = sessionmaker(bind=engine)
session = Session()

with open('./inbox/1C_827496_20231112.json', 'r', encoding='utf-8-sig') as file:
    data = json.load(file)

for model_name, model_info in mappings.items():
    model_class = globals()[model_name]  # Получаем класс модели по имени
    print(model_class)
    json_key = model_info['json_key']
    for chunk in data[json_key]:
        # Создание словаря с данными для модели
        model_data = {model_field: chunk.get(json_field, None) for model_field, json_field in model_info['mapping'].items()}
        model_instance = model_class(**model_data)
        session.add(model_instance)



session.commit()
session.close()