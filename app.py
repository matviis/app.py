import re
from flask import Flask, request, render_template, send_file
import pandas as pd
import os
from io import BytesIO
import zipfile

app = Flask(__name__)

# Путь для сохранения временных файлов
UPLOAD_FOLDER = 'uploads'
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

# Главная страница с формой
@app.route('/')
def index():
    return render_template('index.html')

# Обработка формы и генерация файлов
@app.route('/process-emails', methods=['POST'])
def process_emails():
    # Получение файлов и данных формы
    email_file_1 = request.files['emailFile1']
    percentage_1 = float(request.form.get('percentage1', 100))  # Процент из первого файла
    email_file_2 = request.files.get('emailFile2')  # Второй файл (может быть необязательным)
    percentage_2 = float(request.form.get('percentage2', 0)) if email_file_2 else 0  # Процент из второго файла

    # Получаем план по дням, имя файлов и ключевые слова для удаления доменов
    daily_plan = request.form['dailyPlan'].strip().splitlines()  # Получаем план построчно
    daily_plan = [int(x) for x in daily_plan if x.strip()]  # Преобразуем строки в числа
    base_filename = request.form.get('baseFilename', 'emails')  # Имя файлов по умолчанию
    keyword_input = request.form.get('keyword')  # Ключевые слова для удаления строк (например, домены)

    # Сохраняем загруженные файлы
    file_path_1 = os.path.join(UPLOAD_FOLDER, email_file_1.filename)
    email_file_1.save(file_path_1)
    emails_df_1 = load_and_clean_csv(file_path_1)

    if email_file_2:
        file_path_2 = os.path.join(UPLOAD_FOLDER, email_file_2.filename)
        email_file_2.save(file_path_2)
        emails_df_2 = load_and_clean_csv(file_path_2)
        print(f"Второй файл содержит {len(emails_df_2)} email-адресов.")  # Отладка второго файла
    else:
        emails_df_2 = pd.DataFrame()  # Если второго файла нет, делаем пустой DataFrame

    # Отладочная информация
    print(f"Первый файл содержит {len(emails_df_1)} email-адресов.")
    if not emails_df_2.empty:
        print(f"Второй файл содержит {len(emails_df_2)} email-адресов.")
    else:
        print("Второй файл не предоставлен или пустой.")

    # Фильтрация доменов: пропуск строк с доменами
    if keyword_input:
        keywords = [kw.strip() for kw in keyword_input.split(',')]  # Разделяем ключевые слова по запятой
        emails_df_1 = filter_rows_with_keywords(emails_df_1, keywords)
        if not emails_df_2.empty:
            emails_df_2 = filter_rows_with_keywords(emails_df_2, keywords)

    # Разделение email-адресов по плану на каждый день
    daily_email_batches = split_emails_by_percentage(emails_df_1, emails_df_2, percentage_1, percentage_2, daily_plan)

    # Генерация ZIP файла с результатами
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w') as z:
        for i, batch in enumerate(daily_email_batches, start=1):
            file_name = f'{base_filename}_day_{i}.csv'
            csv_buffer = batch.to_csv(index=False, header=["email"], encoding='utf-8')
            z.writestr(file_name, csv_buffer)

    zip_buffer.seek(0)

    # Отправляем ZIP файл клиенту
    return send_file(zip_buffer, mimetype='application/zip', as_attachment=True, download_name='emails.zip')

def load_and_clean_csv(file_path):
    """
    Загружает CSV и автоматически определяет столбец с почтами.
    """
    try:
        df = pd.read_csv(file_path)
        if df.empty:
            raise ValueError("CSV file is empty.")
        
        # Автоматически определяем, в каком столбце находятся email-адреса
        email_column = detect_email_column(df)
        if email_column is None:
            raise ValueError("No column with valid emails found.")
        
        print(f"Найден столбец с email: {email_column}")
        emails_df = pd.DataFrame(df[email_column], columns=["email"])
        print(emails_df.head())  # Отладка: вывод первых строк с почтами
        return emails_df
    except Exception as e:
        print(f"Error processing file {file_path}: {e}")
        raise

def detect_email_column(df):
    """
    Определяет, какой столбец содержит email-адреса.
    Возвращает название столбца, если найдено.
    """
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'  # Регулярное выражение для email

    for column in df.columns:
        # Проверяем каждый столбец, применяя регулярное выражение для email-адресов
        if df[column].apply(lambda x: isinstance(x, str) and bool(re.match(email_pattern, x))).mean() > 0.5:
            print(f"Столбец с почтами найден: {column}")
            return column

    return None  # Возвращает None, если столбец с почтами не найден

def split_emails_by_percentage(df1, df2, percentage_1, percentage_2, daily_plan):
    """
    Разделяет email-адреса по дням с учётом процентного соотношения.
    df1 и df2 - базы данных, процентное соотношение percentage_1 и percentage_2,
    daily_plan - список с количеством почт на каждый день.
    """
    daily_batches = []
    start_1 = 0
    start_2 = 0

    for emails_per_day in daily_plan:
        # Рассчитываем, сколько почт нужно взять из каждого файла на этот день
        count_1 = int(emails_per_day * (percentage_1 / 100))
        count_2 = emails_per_day - count_1  # Остальное из второго файла

        print(f"На день нужно взять {count_1} из первого файла и {count_2} из второго файла.")

        # Берём нужное количество почт
        batch_1 = df1.iloc[start_1:start_1 + count_1]
        batch_2 = df2.iloc[start_2:start_2 + count_2]

        # Объединяем их
        combined_batch = pd.concat([batch_1, batch_2]).reset_index(drop=True)
        daily_batches.append(combined_batch)

        # Обновляем стартовые индексы для следующего дня
        start_1 += count_1
        start_2 += count_2

    return daily_batches

def filter_rows_with_keywords(emails_df, keywords):
    """
    Пропускает строки, содержащие одно из ключевых слов (доменов).
    """
    pattern = '|'.join(keywords)
    filtered_emails_df = emails_df[~emails_df['email'].str.contains(pattern, case=False, na=False)]
    return filtered_emails_df

# Запуск приложения с учётом порта
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
