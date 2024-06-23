from aiogram import Bot, Dispatcher, types
from aiogram.contrib.middlewares.logging import LoggingMiddleware
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.types import ReplyKeyboardMarkup, ReplyKeyboardRemove
from aiogram.utils import executor
import pandas as pd
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import time
import os
from decouple import config as config_settings
import aiofiles

# Токен вашего Telegram-бота
TELEGRAM_BOT_TOKEN = config_settings('TELEGRAM_BOT_TOKEN')

# Параметры почтового сервера
SMTP_SERVER = 'smtp.gmail.com'
SMTP_PORT = 587
SMTP_USERNAME = config_settings('SMTP_USERNAME')
SMTP_PASSWORD = config_settings('SMTP_PASSWORD')

# Инициализация бота и диспетчера
bot = Bot(token=TELEGRAM_BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)
dp.middleware.setup(LoggingMiddleware())


# Состояния для разговора
class Form(StatesGroup):
    MAIN_MENU = State()
    FILE_NAME = State()
    LINKS_FILE_NAME = State()
    DUPLICATES_FILE_NAME = State()
    CREATE_FILE = State()
    SEND_EMAILS = State()
    UPLOAD_FILE = State()
    ADD_DATA = State()
    GET_LINKS = State()
    CHECK_DUPLICATES = State()
    SEARCH_EMAIL = State()


# Начало работы бота
@dp.message_handler(commands='start', state='*')
async def start(message: types.Message):
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    buttons = ['Создать файл', 'Начать рассылку', 'Вывести ссылки', 'Проверка на дубликаты', 'Поиск по почте']
    keyboard.add(*buttons)
    await message.answer('Здравствуйте! Выберите действие:', reply_markup=keyboard)
    await Form.MAIN_MENU.set()


# Обработка создания файла
@dp.message_handler(lambda message: message.text == 'Создать файл', state=Form.MAIN_MENU)
async def request_file_name(message: types.Message, state: FSMContext):
    await message.answer('Введите название файла:', reply_markup=ReplyKeyboardRemove())
    await Form.FILE_NAME.set()


@dp.message_handler(state=Form.FILE_NAME)
async def create_file(message: types.Message, state: FSMContext):
    file_name = message.text.strip()
    await state.update_data(file_name=file_name, data=[])
    await message.answer('Введите данные в формате:\nИмя\nEmail')
    await Form.ADD_DATA.set()


async def save_data_to_file(name, email, file_name):
    file_path = 'data_storage.txt'
    with open(file_path, 'a') as file:
        file.write(f"{name},{email},{file_name}\n")


@dp.message_handler(state=Form.ADD_DATA)
async def add_data(message: types.Message, state: FSMContext):
    text = message.text
    if text == 'Завершить и создать файл':
        await finish_and_create_file(message, state)
        return

    parts = text.split('\n')
    if len(parts) != 2:
        await message.answer('Неверный формат данных. Введите данные в формате:\nИмя\nEmail')
        return

    name, email = parts
    name = name.title()

    async with state.proxy() as data:
        if 'data' not in data:
            data['data'] = []

        file_name = data['file_name']

        # Проверка, существует ли уже такой человек
        for entry in data['data']:
            if entry['email'].lower() == email.lower():
                await message.answer(f'Человек с email {email} уже существует. Пропускаем.')
                return

        data['data'].append({'name': name, 'email': email, 'file_name': file_name})
        await save_data_to_file(name, email, file_name)

    await message.answer('Данные добавлены. Введите следующие данные или нажмите "Завершить и создать файл".')
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    buttons = ['Завершить и создать файл']
    keyboard.add(*buttons)
    await message.answer('Введите следующие данные или завершите создание файла:', reply_markup=keyboard)


async def finish_and_create_file(message: types.Message, state: FSMContext):
    data = await state.get_data()
    file_name = data.get('file_name', 'output').strip()

    new_subject = 'Здравствуйте, {name}!'
    new_body_template = (
        'Здравствуйте, {name}!\n'
        'Меня зовут Айбек, и я представляю швейные фабрики в Кыргызстане. Мой опыт в швейной сфере '
        'составляет 24 года, и я сам являюсь технологом и закройщиком женской и детской одежды.\n\n'
        'Мы предлагаем сотрудничество по производству качественной одежды на наших фабриках. Наши преимущества:\n\n'
        '- Большой опыт работы: 24 года в швейной индустрии.\n'
        '- Квалифицированные специалисты и современное оборудование.\n'
        '- Юридическое представительство в Москве.\n'
        '- Возможность работы как по наличному, так и безналичному расчету.\n'
        '- Юридические лица в России 🇷🇺 и Кыргызстане 🇰🇬.\n\n'
        'Мы уверены, что наше сотрудничество будет взаимовыгодным и поможет вам, {name}, увеличить ассортимент и '
        'улучшить качество предлагаемой продукции на маркетплейсе Вайлдбериз.\n\n'
        'Будем рады обсудить детали и ответить на любые ваши вопросы.\n'
        'Мои контакты:\n'
        'ismailovaiba79@gmail.com\n'
        '+996552446644\n'
        'https://wa.me/+996552446644\n'
        'https://t.me/Aibalux\n'
        'Whatsapp/Telegram\n\n'
        'С уважением,\n'
        'Айбек'
    )

    df = pd.DataFrame({
        'Email': [entry['email'] for entry in data['data']],
        'Subject': [new_subject.format(name=entry['name']) for entry in data['data']],
        'Body': [new_body_template.format(name=entry['name']) for entry in data['data']]
    })

    new_file_path = f'email_wb/{file_name}.xlsx'
    df.to_excel(new_file_path, index=False)

    with open(new_file_path, 'rb') as file:
        await message.answer_document(file, caption=f'Файл "{file_name}.xlsx" создан.')

    await state.finish()
    await start(message)


# Начало рассылки писем
@dp.message_handler(lambda message: message.text == 'Начать рассылку', state=Form.MAIN_MENU)
async def start_sending_emails(message: types.Message):
    await message.answer('Пожалуйста, отправьте файл с данными для рассылки.', reply_markup=ReplyKeyboardRemove())
    await Form.UPLOAD_FILE.set()


async def check_email_in_history(email_to_check):
    file_path = 'data_storage.txt'
    async with aiofiles.open(file_path, 'r') as file:
        async for line in file:
            parts = line.strip().split(',')
            if len(parts) == 3:  # Учитываем формат с тремя значениями
                name, stored_email, file_name = parts
                if stored_email.lower() == email_to_check.lower():
                    return True
    return False


@dp.message_handler(content_types=types.ContentType.DOCUMENT, state=Form.UPLOAD_FILE)
async def handle_uploaded_file(message: types.Message, state: FSMContext):
    document = message.document
    file_path = await document.download(destination='uploaded_emails.xlsx')

    df = pd.read_excel(file_path.name)

    total_emails = len(df)
    emails_sent = 0
    emails_skipped = 0

    start_time = time.time()

    for index, row in df.iterrows():
        to_address = row['Email']
        subject = row['Subject']
        body = row['Body']

        # Проверяем наличие email в истории
        if await check_email_in_history(to_address):
            await message.answer(f'Email {to_address} уже есть в истории. Пропускаем отправку.')
            emails_skipped += 1
            continue

        try:
            send_email(to_address, subject, body)
            emails_sent += 1
            current_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
            await message.answer(f'Отправлено писем {emails_sent}/{total_emails} на адрес {to_address} в {current_time}')
        except Exception as e:
            await message.answer(f'Ошибка при отправке письма на адрес {to_address}: {e}')

    end_time = time.time()
    total_time = end_time - start_time

    await message.answer(f'Всего отправлено {emails_sent} писем за {total_time:.2f} секунд.')
    if emails_skipped > 0:
        await message.answer(f'Пропущено {emails_skipped} писем, так как они уже присутствуют в истории.')

    await state.finish()
    await start(message)


# Отправка email
def send_email(to_address, subject, body):
    msg = MIMEMultipart()
    msg['From'] = SMTP_USERNAME
    msg['To'] = to_address
    msg['Subject'] = subject

    msg.attach(MIMEText(body, 'plain'))

    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_USERNAME, SMTP_PASSWORD)
        server.send_message(msg)


@dp.message_handler(lambda message: message.text == 'Вывести ссылки', state=Form.MAIN_MENU)
async def request_links_file_name(message: types.Message, state: FSMContext):
    await message.answer('Введите название файла для сохранения ссылок:', reply_markup=ReplyKeyboardRemove())
    await Form.LINKS_FILE_NAME.set()


@dp.message_handler(state=Form.LINKS_FILE_NAME)
async def handle_links_file_name(message: types.Message, state: FSMContext):
    file_name = message.text.strip()

    await state.update_data(links_file_name=file_name)
    await message.answer('Пожалуйста, отправьте Excel файл с данными для получения ссылок.')

    await Form.GET_LINKS.set()


@dp.message_handler(content_types=types.ContentType.DOCUMENT, state=Form.GET_LINKS)
async def handle_links_file(message: types.Message, state: FSMContext):
    document = message.document
    file_path = await document.download(destination='uploaded_data.xlsx')

    data = await state.get_data()
    links_file_name = data.get('links_file_name', 'links').strip()

    df = pd.read_excel(file_path.name)
    sku_list = df['SKU'].tolist()

    base_url = "https://www.wildberries.ru/catalog/"
    links = []

    for sku in sku_list:
        url = base_url + str(sku) + "/detail.aspx"
        links.append(url)

    # Удаление дубликатов и создание строки с уникальными ссылками с отступами
    unique_links = list(set(links))  # преобразуем в множество, чтобы удалить дубликаты, затем обратно в список

    # Запись уникальных ссылок в текстовый файл
    links_str = ""
    for idx, link in enumerate(unique_links, start=1):
        links_str += f"{idx}. {link}\n"

    links_file_path = f'info_wb/{links_file_name}.txt'
    with open(links_file_path, 'w') as file:
        file.write(links_str)

    # Сохранение данных без дубликатов обратно в Excel файл
    output_file_path = f'info_wb/without_duplicates_{links_file_name}.xlsx'
    df.to_excel(output_file_path, index=False)

    with open(links_file_path, 'rb') as file:
        await message.answer_document(file, caption=f'Ссылки сохранены в "{links_file_name}.txt"')

    await message.answer(f'Найдено артикулов: {len(sku_list)}')
    await message.answer(f"Найдено дубликатов: {len(links) - len(unique_links)}")

    await state.finish()
    await start(message)


@dp.message_handler(lambda message: message.text == 'Проверка на дубликаты', state=Form.MAIN_MENU)
async def request_duplicates_file_name(message: types.Message, state: FSMContext):
    await message.answer('Введите название файла для сохранения без дубликатов:', reply_markup=ReplyKeyboardRemove())
    await Form.DUPLICATES_FILE_NAME.set()


@dp.message_handler(state=Form.DUPLICATES_FILE_NAME)
async def handle_duplicates_file_name(message: types.Message, state: FSMContext):
    file_name = message.text.strip()

    await state.update_data(duplicates_file_name=file_name)
    await message.answer('Пожалуйста, отправьте TXT файл с данными для проверки на дубликаты.')

    await Form.CHECK_DUPLICATES.set()


@dp.message_handler(content_types=types.ContentType.DOCUMENT, state=Form.CHECK_DUPLICATES)
async def handle_duplicates_file(message: types.Message, state: FSMContext):
    document = message.document
    file_path = await document.download(destination='uploaded_links.txt')

    data = await state.get_data()
    duplicates_file_name = data.get('duplicates_file_name', 'duplicates').strip()

    with open(file_path.name, 'r') as file:
        links = file.readlines()

    # Удаление дубликатов и подсчет количества
    unique_links = list(set([link.strip() for link in links]))
    duplicates_count = len(links) - len(unique_links)

    # Запись уникальных ссылок обратно в текстовый файл с нумерацией
    output_file_path = f'info_wb/{duplicates_file_name}.txt'
    with open(output_file_path, 'w') as file:
        for index, link in enumerate(unique_links, start=1):
            file.write(f"{index}. {link}\n")

    await message.answer_document(open(output_file_path, 'rb'), caption=f'Ссылки без дубликатов сохранены в "{duplicates_file_name}.txt"')
    await message.answer(f'Всего ссылок: {len(links)}')
    await message.answer(f'Найдено дубликатов: {duplicates_count}')
    await message.answer(f'Уникальных ссылок: {len(unique_links)}')

    await state.finish()
    await start(message)


# Обработка поиска по email
@dp.message_handler(lambda message: message.text == 'Поиск по почте', state=Form.MAIN_MENU)
async def search_by_email(message: types.Message):
    await message.answer('Введите email для поиска:', reply_markup=ReplyKeyboardRemove())
    await Form.SEARCH_EMAIL.set()


async def search_data_by_email(email_to_search):
    results = []
    files_with_email = []

    # Поиск по файлам
    file_path = 'data_storage.txt'
    with open(file_path, 'r') as file:
        for line in file:
            parts = line.strip().split(',')
            if len(parts) == 3:  # Учитываем также название файла
                name, email, file_name = parts
                if email.lower() == email_to_search.lower():
                    results.append({'name': name, 'email': email, 'file_name': file_name})
                    files_with_email.append(file_name)

    return results, files_with_email


@dp.message_handler(state=Form.SEARCH_EMAIL)
async def handle_search_email(message: types.Message, state: FSMContext):
    email_to_search = message.text.strip().lower()

    results, files_with_email = await search_data_by_email(email_to_search)

    if results:
        response = f'Email найден в файлах:\n'
        for filename in files_with_email:
            response += f'- {filename}\n'

        response += '\nНайденные записи:\n'
        for result in results:
            response += f'Имя: {result["name"]}, Email: {result["email"]}, Название файла: {result["file_name"]}\n'
    else:
        response = 'Ничего не найдено.'

    await message.answer(response)

    await state.finish()
    await start(message)

# Основная функция запуска бота
if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True)