import requests
import json

def fetch_hh_data(companies):
    all_data = {}

    for company in companies:
        url = f'https://api.hh.ru/vacancies?employer_id={company["id"]}'
        response = requests.get(url)
        data = response.json()
        all_data[company['name']] = data

    with open('hh_data.json', 'w') as json_file:
        json.dump(all_data, json_file, indent=2)

if __name__ == "__main__":
    selected_companies = [
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

    fetch_hh_data(selected_companies)
