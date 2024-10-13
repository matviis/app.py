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
    percentage_1 = float(request.form.get('percentage1', 100))  # По умолчанию 100%
    
    # Получение второго файла и процента, если предоставлено
    email_file_2 = request.files.get('emailFile2')
    percentage_2 = float(request.form.get('percentage2', 0)) if email_file_2 else 0

    # Получаем план по дням, имя файлов и ключевые слова (домены)
    daily_plan = request.form['dailyPlan'].strip().splitlines()  # Получаем план построчно
    daily_plan = [int(x) for x in daily_plan if x.strip()]  # Преобразуем строки в числа, убираем пустые строки
    base_filename = request.form.get('baseFilename', 'emails')  # Имя файлов, по умолчанию "emails"
    keyword_input = request.form.get('keyword')  # Получаем ключевые слова

    # Сохранение загруженных файлов
    file_path_1 = os.path.join(UPLOAD_FOLDER, email_file_1.filename)
    email_file_1.save(file_path_1)
    emails_df_1 = load_and_clean_csv(file_path_1)
    segment_1 = select_random_segment(emails_df_1, percentage_1)

    if email_file_2:
        file_path_2 = os.path.join(UPLOAD_FOLDER, email_file_2.filename)
        email_file_2.save(file_path_2)
        emails_df_2 = load_and_clean_csv(file_path_2)
        segment_2 = select_random_segment(emails_df_2, percentage_2)
        combined_emails_df = pd.concat([segment_1, segment_2])
    else:
        combined_emails_df = segment_1

    # Удаление строк, содержащих ключевые слова (домены)
    if keyword_input:
        keywords = [kw.strip() for kw in keyword_input.split(',')]  # Разделяем ключевые слова по запятой
        combined_emails_df = remove_rows_with_keywords(combined_emails_df, keywords)

    # Разделение по дневному плану
    daily_email_batches = split_emails_by_plan(combined_emails_df, daily_plan)

    # Сохранение каждого дня в отдельный CSV с пользовательским именем файлов
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w') as z:
        for i, batch in enumerate(daily_email_batches, start=1):
            file_name = f'{base_filename}_day_{i}.csv'  # Измененное имя файла
            csv_buffer = batch.to_csv(index=False, header=["email"], encoding='utf-8')
            z.writestr(file_name, csv_buffer)
    
    zip_buffer.seek(0)

    # Возврат сгенерированного ZIP файла
    return send_file(zip_buffer, mimetype='application/zip', as_attachment=True, download_name='emails.zip')

def load_and_clean_csv(file_path):
    """
    Загружает CSV и автоматически определяет столбец с почтами.
    """
    try:
        df = pd.read_csv(file_path)
        if df.empty:
            raise ValueError("CSV file is empty.")
        
        # Определим столбец с почтами
        email_column = detect_email_column(df)
        if email_column is None:
            raise ValueError("No column with valid emails found.")
        
        emails_df = pd.DataFrame(df[email_column], columns=["email"])
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
        # Проверяем каждый столбец
        if df[column].apply(lambda x: isinstance(x, str) and re.match(email_pattern, x)).mean() > 0.5:
            # Если более 50% значений в столбце соответствуют шаблону email, считаем его столбцом с почтами
            return column

    return None  # Возвращает None, если столбец с почтами не найден

def split_emails_by_plan(emails_df, daily_plan):
    """
    Разделяет emails_df на блоки в зависимости от плана.
    Возвращает список DataFrame'ов для каждого дня.
    """
    start = 0
    daily_email_batches = []

    for emails_per_day in daily_plan:
        end = start + emails_per_day
        daily_email_batches.append(emails_df.iloc[start:end])
        start = end
    
    return daily_email_batches

def select_random_segment(emails_df, percentage):
    """
    Выбирает случайный процент сегмента пользователей из базы данных.
    """
    total_emails = len(emails_df)
    sample_size = int(total_emails * (percentage / 100))
    return emails_df.sample(n=sample_size)

def remove_rows_with_keywords(emails_df, keywords):
    """
    Удаляет строки, содержащие одно из ключевых слов (доменов), и смещает оставшиеся строки вверх.
    """
    pattern = '|'.join(keywords)  # Объединяем ключевые слова через '|', чтобы использовать в регулярном выражении
    filtered_emails_df = emails_df[~emails_df['email'].str.contains(pattern, case=False, na=False)]
    filtered_emails_df.reset_index(drop=True, inplace=True)  # Сброс индексов, чтобы не было пропусков
    return filtered_emails_df

# Запуск приложения с учетом порта, установленного Render или локального порта 5000
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
