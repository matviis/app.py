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
    email_file_2 = request.files['emailFile2']
    daily_plan = request.form['dailyPlan'].strip().splitlines()  # Получаем план построчно
    daily_plan = [int(x) for x in daily_plan if x.strip()]  # Преобразуем строки в числа, убираем пустые строки

    # Получаем проценты для разделения между двумя базами данных
    percentage_1 = float(request.form.get('percentage1', 0))
    percentage_2 = float(request.form.get('percentage2', 0))

    # Сохраняем загруженные файлы
    file_path_1 = os.path.join(UPLOAD_FOLDER, email_file_1.filename)
    file_path_2 = os.path.join(UPLOAD_FOLDER, email_file_2.filename)
    email_file_1.save(file_path_1)
    email_file_2.save(file_path_2)

    # Загружаем и очищаем данные из обоих файлов
    emails_df_1 = load_and_clean_csv(file_path_1)
    emails_df_2 = load_and_clean_csv(file_path_2)

    # Получаем нужные проценты из каждой базы
    segment_1 = select_random_segment(emails_df_1, percentage_1)
    segment_2 = select_random_segment(emails_df_2, percentage_2)

    # Объединяем два сегмента в один DataFrame
    combined_emails_df = pd.concat([segment_1, segment_2])

    # Разделение по дневному плану
    daily_email_batches = split_emails_by_plan(combined_emails_df, daily_plan)

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

def select_random_segment(emails_df, percentage):
    """
    Выбирает случайный процент сегмента пользователей из базы данных.
    """
    total_emails = len(emails_df)
    sample_size = int(total_emails * (percentage / 100))
    return emails_df.sample(n=sample_size)

# Запуск приложения с учетом порта, установленного Render или локального порта 5000
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
