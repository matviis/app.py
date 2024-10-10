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
    # Получение файла и данных формы
    email_file = request.files['emailFile']
    daily_plan = request.form['dailyPlan'].strip().splitlines()  # Получаем план построчно
    daily_plan = [int(x) for x in daily_plan if x.strip()]  # Преобразуем строки в числа, убираем пустые строки

    # Сохранение загруженного файла
    file_path = os.path.join(UPLOAD_FOLDER, email_file.filename)
    email_file.save(file_path)

    # Загрузка CSV и очистка
    emails_df = load_and_clean_csv(file_path)

    # Разделение по дневному плану
    daily_email_batches = split_emails_by_plan(emails_df, daily_plan)

    # Сохранение каждого дня в отдельный CSV
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w') as z:
        for i, batch in enumerate(daily_email_batches, start=1):
            file_name = f'day_{i}_emails.csv'
            csv_buffer = batch.to_csv(index=False, header=["email"], encoding='utf-8')
            z.writestr(file_name, csv_buffer)
    
    zip_buffer.seek(0)

    # Возврат сгенерированного ZIP файла
    return send_file(zip_buffer, mimetype='application/zip', as_attachment=True, download_name='emails.zip')

def load_and_clean_csv(file_path):
    """
    Загружает CSV и оставляет только второй столбец с почтами.
    """
    df = pd.read_csv(file_path)
    email_column = df.iloc[:, 1]  # Второй столбец
    emails_df = pd.DataFrame(email_column, columns=["email"])
    return emails_df

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

# Запуск приложения с учетом порта, установленного Render или локального порта 5000
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
