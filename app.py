import pandas as pd
import re

# Функция для поиска столбца с email-адресами
def find_email_column(df):
    email_pattern = re.compile(r'^[\w\.-]+@[\w\.-]+\.\w+$')  # Регулярное выражение для email
    
    for column in df.columns:
        # Проверяем, есть ли в столбце хотя бы одно значение, соответствующее формату email
        if df[column].astype(str).str.match(email_pattern).any():
            print(f"Столбец с почтами найден: {column}")
            return df[column]
    
    print("Столбец с почтами не найден.")
    return pd.Series([], dtype=str)

# Функция для обработки email-адресов по дневному плану
def process_emails_v2(emails_first, emails_second, day_plan):
    total_emails_first = len(emails_first)
    total_emails_second = len(emails_second)
    
    results = []
    
    for percentage in day_plan:
        first_file_count = percentage[0]
        second_file_count = percentage[1]
        
        if total_emails_first >= first_file_count and total_emails_second >= second_file_count:
            # Берем нужное количество email из обоих файлов
            selected_first = emails_first.iloc[:first_file_count]
            selected_second = emails_second.iloc[:second_file_count]
            results.append((selected_first, selected_second))
        elif total_emails_first >= first_file_count:
            # Если второго файла не хватает, берем только из первого
            selected_first = emails_first.iloc[:first_file_count]
            selected_second = pd.Series([], dtype=str)  # Пустой набор из второго файла
            results.append((selected_first, selected_second))
        else:
            print(f"Недостаточно данных в первом файле для {first_file_count} email-адресов.")
    
    return results

# Пример загрузки файлов
first_file_path = 'path_to_first_file.csv'  # Путь к первому файлу
second_file_path = 'path_to_second_file.csv'  # Путь ко второму файлу

first_file = pd.read_csv(first_file_path)  # Загрузка первого файла
second_file = pd.read_csv(second_file_path)  # Загрузка второго файла

# Находим столбцы с email в обоих файлах
emails_first = find_email_column(first_file)  # Поиск email-адресов в первом файле
emails_second = find_email_column(second_file)  # Поиск email-адресов во втором файле

# План на день (например, [(10, 40), (20, 80), ...])
day_plan = [(10, 40), (20, 80), (40, 160), (60, 240)]  # Пример плана выборки на каждый день

# Обработка email-адресов по дневному плану
results_v2 = process_emails_v2(emails_first, emails_second, day_plan)

# Отображаем результаты
for i, result in enumerate(results_v2):
    selected_first, selected_second = result
    print(f"\nДень {i + 1}:")
    print(f"Первые {len(selected_first)} адресов из первого файла:")
    print(selected_first)
    print(f"Первые {len(selected_second)} адресов из второго файла:")
    print(selected_second)
