import telegram
from flask import Flask, request
import json

# Замените на ваш токен и URL webhook-а
bot = telegram.Bot(token='YOUR_TOKEN')
app = Flask(__name__)

# Словарь для сопоставления уровней тревоги с чатами
alert_levels = {
    'critical': 'CHAT_ID_CRITICAL',
    'warning': 'CHAT_ID_WARNING',
    'info': 'CHAT_ID_INFO'
}

# Функция для создания клавиатуры с кнопками
def create_keyboard(alert_id):
    keyboard = [[telegram.InlineKeyboardButton("Подтвердить", callback_data=f"confirm_{alert_id}"),
                 telegram.InlineKeyboardButton("Отменить", callback_data=f"cancel_{alert_id}")]]
    reply_markup = telegram.InlineKeyboardMarkup(keyboard)
    return reply_markup

@app.route('/alert', methods=['POST'])
def handle_alert():
    data = request.json
    alert_level = data['status']
    alert_id = data['id']  # Предполагаем, что у каждого алерта есть уникальный ID
    description = data['summary']

    # Определяем чат для отправки уведомления
    chat_id = alert_levels.get(alert_level, 'DEFAULT_CHAT_ID')

    # Формируем сообщение с кнопками
    text = f"Новый алерт: {alert_level}\n{description}"
    reply_markup = create_keyboard(alert_id)
    bot.send_message(chat_id=chat_id, text=text, reply_markup=reply_markup)

    return 'OK', 200

# Обработчик callback-запросов
@app.route('/callback', methods=['POST'])
def handle_callback_query():
    query = telegram.Update.de_serialize(request.get_data()).callback_query
    data = query.data
    chat_id = query.message.chat.id

    if data.startswith('confirm_'):
        alert_id = data.split('_')[1]
        # Здесь можно добавить логику подтверждения алерта (например, отправить сообщение в другой сервис)
        bot.edit_message_text(chat_id=chat_id, message_id=query.message.message_id, text="Алерт подтвержден")
    elif data.startswith('cancel_'):
        alert_id = data.split('_')[1]
        # Здесь можно добавить логику отмены алерта
        bot.edit_message_text(chat_id=chat_id, message_id=query.message.message_id, text="Алерт отменен")

    return 'OK', 200

if __name__ == '__main__':
    app.run()
