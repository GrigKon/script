import websocket
import json
import sqlite3
from threading import Timer

# Настройки подключения к WebSocket API ALOR
WEBSOCKET_URL = "wss://api.alor.ru/ws"
ACCESS_TOKEN = "your_access_token"  # Замените на ваш токен доступа

# Настройки базы данных SQLite3
DATABASE = "database.db"

# Функция для создания таблицы в базе данных
def create_table():
    conn = None
    try:
        conn = sqlite3.connect(DATABASE)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS quotes (
                symbol TEXT,
                time INTEGER,
                bid REAL,
                ask REAL,
                last_price REAL,
                volume INTEGER
            )
        ''')
        conn.commit()
    except sqlite3.Error as e:
        print(f"Ошибка при создании таблицы: {e}")
    finally:
        if conn:
            conn.close()

# Функция для записи данных в базу данных 
def save_quote_to_db(quote):
    conn = None
    try:
        # Проверка на наличие всех необходимых полей в данных
        if not all(key in quote for key in ['symbol', 'time', 'bid', 'ask', 'last_price', 'volume']):
            print("Отсутствуют необходимые данные в котировке")
            return

        # Проверка на null значения
        if any(quote[key] is None for key in ['symbol', 'time', 'bid', 'ask', 'last_price', 'volume']):
            print("Обнаружены null значения в котировке")
            return

        conn = sqlite3.connect(DATABASE)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO quotes (symbol, time, bid, ask, last_price, volume)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (quote['symbol'], quote['time'], quote['bid'], quote['ask'], quote['last_price'], quote['volume']))
        conn.commit()
    except sqlite3.Error as e:
        print(f"Ошибка при записи в базу данных: {e}")
    finally:
        if conn:
            conn.close()

# Функция для обработки сообщений от WebSocket
def on_message(ws, message):
    try:
        data = json.loads(message)
        if 'data' in data:
            quote = data['data']
            print(f"Received quote: {quote}")
            save_quote_to_db(quote)
        else:
            print("Отсутствуют данные в сообщении")
    except json.JSONDecodeError as e:
        print(f"Ошибка декодирования JSON: {e}")
    except KeyError as e:
        print(f"Отсутствует ключ в данных: {e}")

# Функция для обработки ошибок
def on_error(ws, error):
    print(f"WebSocket error: {error}")

# Функция для обработки закрытия соединения
def on_close(ws):
    print("WebSocket connection closed")

# Функция для отправки подписки на котировки
def subscribe_to_quotes(ws):
    try:
        subscription_message = {
            "opcode": "QuotesSubscribe",
            "code": "ALL",  # Подписка на все инструменты
            "exchange": "MOEX",  # Московская биржа
            "format": "Simple",
            "token": ACCESS_TOKEN,
            "guid": "quotes_subscription"
        }
        ws.send(json.dumps(subscription_message))
    except websocket.WebSocketConnectionClosedException as e:
        print(f"Ошибка при отправке подписки: {e}")
    except Exception as e:
        print(f"Неожиданная ошибка при отправке подписки: {e}")

# Функция для установки соединения и подписки на котировки
def on_open(ws):
    print("WebSocket connection opened")
    subscribe_to_quotes(ws)
    # Запуск таймера для повторной подписки каждые 10 минут
    Timer(600, subscribe_to_quotes, args=[ws]).start()

# Основная функция
def main():
    create_table()
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp(WEBSOCKET_URL,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    ws.on_open = on_open
    ws.run_forever()

if __name__ == "__main__":
    main()