import json
import requests
from db_manager import DBManager


def get_hh_data(company):
    url = f'https://api.hh.ru/vacancies?employer_id={company["id"]}'

    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        if response.status_code == 200 and data.get('items'):
            print(f"Для компании {company['name']} успешно получены данные.")
            return {company['name']: {"items": data['items']}}
        else:
            print(f"Для компании {company['name']} отсутствуют вакансии или получены пустые данные.")
    except requests.RequestException as e:
        print(f"Ошибка во время запроса для компании {company['name']}: {e}")

    return None


def should_clear_tables():
    while True:
        answer = input("Хотите очистить таблицы перед добавлением новых данных? (y/n): ").lower()
        if answer == 'y':
            return True
        elif answer == 'n':
            return False
        else:
            print("Пожалуйста, введите 'y' для подтверждения или 'n' для отмены.")


def update_json_file(data):
    with open('hh_data.json', 'w', encoding='utf-8') as json_file:
        json.dump(data, json_file, ensure_ascii=False, indent=2)


def run_project():
    companies = [
        {"id": 1740, "name": "Yandex"},
        {"id": 87021, "name": "Wildberries"},
        {"id": 15478, "name": "VK"},
        {"id": 3529, "name": "Sberbank"},
        {"id": 4181, "name": "VTB"},
        {"id": 3776, "name": "MTS"},
        {"id": 39305, "name": "Gazprom"},
        {"id": 6596, "name": "Rosneft"},
        {"id": 80, "name": "Alfabank"},
        {"id": 2180, "name": "Ozon"},
    ]

    db_manager = DBManager()
    db_manager.create_tables()

    if should_clear_tables():
        db_manager.clear_tables()

    hh_data = []

    print("Выберите компании для загрузки данных:")
    for i, company in enumerate(companies, 1):
        print(f"{i}: {company['name']}")

    while True:
        selected_companies = input("Введите номера компаний через запятую (например, 1,3,5): ")
        selected_indices = [int(index) for index in selected_companies.split(',') if index.isdigit()]

        invalid_indices = [index for index in selected_indices if not (1 <= index <= len(companies))]
        if invalid_indices:
            print(f"Неверные индексы: {', '.join(map(str, invalid_indices))}. Повторите ввод.")
        else:
            break

    for index in selected_indices:
        company_data = get_hh_data(companies[index - 1])
        if company_data:
            hh_data.append(company_data)

    if hh_data:
        print("Для следующих компаний успешно получены данные:")
        for company_data in hh_data:
            company_name = list(company_data.keys())[0]
            print(f" - {company_name}")
            db_manager.insert_data(company_data)

        # Обновление JSON-файла
        existing_data = {}

        try:
            with open('hh_data.json', 'r', encoding='utf-8') as json_file:
                existing_data = json.load(json_file)
        except FileNotFoundError:
            print("Файл не существует. Продолжение работы без загрузки предыдущих данных.")
        except json.JSONDecodeError:
            print("Ошибка при декодировании JSON. Продолжение работы без загрузки предыдущих данных.")
            try:
                with open('hh_data.json', 'r', encoding='cp1251') as json_file:
                    existing_data = json.load(json_file)
            except FileNotFoundError:
                pass 

        for company_data in hh_data:
            existing_data.update(company_data)
        update_json_file(existing_data)
    else:
        print("Отсутствуют данные для обработки.")

    companies_count, vacancies_count = db_manager.get_companies_and_vacancies_count()
    print(f"Общее количество компаний: {companies_count}")
    print(f"Общее количество вакансий: {vacancies_count}")

    avg_salary = db_manager.get_avg_salary()
    print(f"Средняя зарплата по вакансиям: {avg_salary}")

    high_salary_vacancies = db_manager.get_vacancies_with_higher_salary()
    print("\nВакансии с зарплатой выше средней:")
    for vacancy in high_salary_vacancies:
        print(vacancy)

    keyword = input("Введите ключевое слово для поиска вакансий: ")
    keyword_vacancies = db_manager.get_vacancies_with_keyword(keyword)
    print(f"\nВакансии с ключевым словом '{keyword}':")
    for vacancy in keyword_vacancies:
        print(vacancy)

    db_manager.close_connection()


if __name__ == "__main__":
    run_project()
