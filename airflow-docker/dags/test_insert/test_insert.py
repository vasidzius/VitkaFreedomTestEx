import psycopg2

# Класс для работы с базой данных
class PostgreSQLConnector:
    def __init__(self, dbname, user, password, host):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.conn = None

    # Метод для подключения к базе данных
    def connect(self):
        try:
            self.conn = psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host
            )
            print("Успешное подключение к базе данных.")
        except (Exception, psycopg2.Error) as error:
            print("Ошибка при подключении к базе данных:", error)

    # Метод для записи данных в базу данных
    def write_to_db(self, number, text):
        if self.conn is None or self.conn.closed:
            self.connect()
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO json_platform.test_table (number, text)
                VALUES (%s, %s);
            """, (number, text))
            self.conn.commit()
            print("Данные успешно записаны в таблицу.")
        except (Exception, psycopg2.Error) as error:
            print("Ошибка при записи данных в таблицу:", error)
        finally:
            if cursor is not None:
                cursor.close()
            if self.conn is not None:
                self.conn.close()
                print("Соединение с базой данных закрыто.")

# Создание экземпляра класса и запись данных в базу данных
if __name__ == "__main__":
    connector = PostgreSQLConnector(
        dbname="postgres",
        user="postgres",
        password="",
        host=""
    )
    connector.write_to_db(123, "example text")
