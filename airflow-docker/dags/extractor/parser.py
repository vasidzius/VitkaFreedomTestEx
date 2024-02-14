import os
from sqlalchemy.orm import sessionmaker
from .alchemy import engine  # Убедитесь, что engine правильно импортирован из вашего файла alchemy.py
from .alchemy import AccruedCommissions, RepoDocuments, TradeDocuments, FinancialInstrumentsMovement, MoneyMovement  # Импорт модели
from .mappings import mappings
import json

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
