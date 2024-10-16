import pandas as pd
from flask import Flask, request, render_template, send_file
import os
from io import BytesIO

app = Flask(__name__)

# Путь для сохранения временных файлов
UPLOAD_FOLDER = 'uploads'
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

# Главная страница с вкладками
@app.route('/')
def index():
    return render_template('index.html', page='process_emails')

# Вкладка для обработки почт
@app.route('/process-emails', methods=['POST'])
def process_emails():
    # Получение файлов и данных формы
    email_file_1 = request.files['emailFile1']
    percentage_1 = float(request.form.get('percentage1', 100))  # Процент из первого файла
    email_file_2 = request.files.get('emailFile2')  # Второй файл (может быть необязательным)
    percentage_2 = float(request.form.get('percentage2', 0)) if email_file_2 else 0  # Процент из второго файла

    # Получаем план по дням и имя файлов
    daily_plan = request.form['dailyPlan'].strip().splitlines()  # Получаем план построчно
    daily_plan = [int(x) for x in daily_plan if x.strip()]  # Преобразуем строки в числа
    base_filename = request.form.get('baseFilename', 'emails')  # Имя файлов по умолчанию

    # Сохраняем загруженные файлы
    file_path_1 = os.path.join(UPLOAD_FOLDER, email_file_1.filename)
    email_file_1.save(file_path_1)
    emails_df_1 = load_emails_from_second_column(file_path_1)

    if email_file_2:
        file_path_2 = os.path.join(UPLOAD_FOLDER, email_file_2.filename)
        email_file_2.save(file_path_2)
        emails_df_2 = load_emails_from_second_column(file_path_2)
        print(f"Второй файл содержит {len(emails_df_2)} email-адресов.")  # Отладка второго файла
    else:
        emails_df_2 = pd.DataFrame()  # Если второго файла нет, делаем пустой DataFrame

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

# Вкладка для удаления дубликатов
@app.route('/remove-duplicates', methods=['GET', 'POST'])
def remove_duplicates():
    if request.method == 'POST':
        # Получаем загруженный файл
        file = request.files['emailFile']
        if not file:
            return "No file uploaded.", 400
        
        # Сохраняем файл
        file_path = os.path.join(UPLOAD_FOLDER, file.filename)
        file.save(file_path)

        # Загружаем CSV и удаляем дубликаты
        try:
            df = pd.read_csv(file_path)
            email_column = df.columns[1]  # Предполагаем, что email-адреса находятся во втором столбце
            df = df.drop_duplicates(subset=email_column)

            # Удаляем пробелы после удаления дубликатов
            df = df.dropna().reset_index(drop=True)

            # Генерируем CSV без дубликатов
            output = BytesIO()
            df.to_csv(output, index=False)
            output.seek(0)

            # Отправляем файл клиенту
            return send_file(output, mimetype='text/csv', as_attachment=True, download_name='cleaned_emails.csv')
        except Exception as e:
            return f"An error occurred: {str(e)}", 500

    return render_template('remove_duplicates.html')

def load_emails_from_second_column(file_path):
    """
    Загружает CSV и возвращает только второй столбец, в котором находятся email-адреса.
    """
    try:
        df = pd.read_csv(file_path)
        emails_df = pd.DataFrame(df.iloc[:, 1], columns=["email"])  # Возвращаем только второй столбец
        return emails_df
    except Exception as e:
        print(f"Error processing file {file_path}: {e}")
        raise

def split_emails_by_percentage(df1, df2, percentage_1, percentage_2, daily_plan):
    """
    Разделяет email-адреса по дням с учётом процентного соотношения.
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

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
