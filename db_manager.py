import json
import psycopg2
from configparser import ConfigParser


class DBManager:
    def __init__(self, config_file='config.ini'):
        self.config = self.read_config(config_file)
        self.conn = psycopg2.connect(**self.config)
        self.cur = self.conn.cursor()

    def read_config(self, filename='config.ini', section='postgresql'):
        parser = ConfigParser()
        parser.read(filename)

        db_config = {}
        if parser.has_section(section):
            params = parser.items(section)
            db_config = {param[0]: param[1] for param in params}
        else:
            raise Exception(f'Section {section} not found in the {filename} file.')

        return db_config

    def create_tables(self):
        create_companies_table = """
        CREATE TABLE IF NOT EXISTS companies (
            id SERIAL PRIMARY KEY,
            company_id INTEGER,
            name VARCHAR(255) NOT NULL,
            url VARCHAR(255)
        );
        """
        create_vacancies_table = """
        CREATE TABLE IF NOT EXISTS vacancies (
            id SERIAL PRIMARY KEY,
            company_id INTEGER,
            name VARCHAR(255) NOT NULL,
            url VARCHAR(255),
            salary JSONB
        );
        """
        self.cur.execute(create_companies_table)
        self.cur.execute(create_vacancies_table)
        self.conn.commit()

    def clear_tables(self):
        self.cur.execute("DELETE FROM companies;")
        self.cur.execute("DELETE FROM vacancies;")
        self.conn.commit()

    def insert_company(self, company_id, company_name, alternate_url):
        self.cur.execute(
            "INSERT INTO companies (company_id, name, url) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
            (company_id, company_name, alternate_url)
        )

    def insert_vacancy(self, company_id, name, url, salary):
        try:
            salary_json = json.loads(salary)
        except (json.JSONDecodeError, TypeError):
            salary_json = {"value": "Данные не указаны"}

        self.cur.execute(
            "INSERT INTO vacancies (company_id, name, url, salary) VALUES (%s, %s, %s, %s)",
            (company_id, name, url, json.dumps(salary_json))
        )

    def insert_data(self, companies_data):
        for company_name, company_info in companies_data.items():
            items = company_info.get('items', [])
            if items:
                employer_info = items[0].get('employer', {})
                company_id = employer_info.get('id')

                if company_id:
                    self.insert_company(
                        company_id, company_name, employer_info.get('alternate_url', None)
                    )

                    for vacancy in items:
                        salary_data = vacancy.get('salary', {})
                        if salary_data is not None and 'from' in salary_data:
                            salary_json = json.dumps(salary_data)
                        else:
                            salary_json = 'Данные не указаны'

                        self.insert_vacancy(
                            company_id, vacancy.get('name', ''), vacancy.get('url', ''), salary_json
                        )

                    print(f"Для компании {company_name} добавлены вакансии. Общее количество вакансий: {len(items)}")
                else:
                    print(f"Для компании {company_name} не найден ID работодателя.")
            else:
                print(f"Для компании {company_name} отсутствуют вакансии.")

        self.conn.commit()

    def get_companies_and_vacancies_count(self):
        self.cur.execute("SELECT COUNT(DISTINCT id) FROM companies")
        companies_count = self.cur.fetchone()[0]

        self.cur.execute("SELECT COUNT(DISTINCT id) FROM vacancies")
        vacancies_count = self.cur.fetchone()[0]

        return companies_count, vacancies_count

    def get_all_vacancies(self):
        self.cur.execute("SELECT * FROM vacancies")
        return self.cur.fetchall()

    def get_companies_list(self):
        self.cur.execute("SELECT id, name FROM companies")
        return self.cur.fetchall()

    def get_company_vacancies_count(self, company_id):
        self.cur.execute("SELECT COUNT(*) FROM vacancies WHERE company_id = %s", (company_id,))
        return self.cur.fetchone()[0]

    def close_connection(self):
        self.cur.close()
        self.conn.close()


if __name__ == "__main__":
    with open('hh_data.json', 'r') as json_file:
        hh_data = json.load(json_file)

    db_manager = DBManager()
    db_manager.create_tables()
    db_manager.insert_data(hh_data)

    print("Данные успешно загружены в базу данных.")

    companies_count, vacancies_count = db_manager.get_companies_and_vacancies_count()
    print(f"Общее количество компаний: {companies_count}")
    print(f"Общее количество вакансий: {vacancies_count}")

    action = input(
        "Выберите действие:\n1: Получить список всех компаний и количество вакансий\n2: Выход\nВведите номер действия: ")

    if action == "1":
        companies_list = db_manager.get_companies_list()
        print("\nСписок компаний и количество вакансий:")
        for company in companies_list:
            company_id, company_name = company
            company_vacancies_count = db_manager.get_company_vacancies_count(company_id)
            print(f"Компания: {company_name}, Количество вакансий: {company_vacancies_count}")

    db_manager.close_connection()
