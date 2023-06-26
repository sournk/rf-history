import logging
from telegram import Update
from telegram.ext import filters, ApplicationBuilder, ContextTypes, CommandHandler, MessageHandler

import config
import model


logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    answer = 'Send me your roboforex.com Excel history export to analyze'
    await context.bot.send_message(chat_id=update.effective_chat.id, 
                                   text=answer)

async def downloader(update, context):
    # logging.warn(from_user)
    # print(update.message)

    try:
        await model.get_file_from_message(update.message, context)    
        await update.message.reply_text("Спасибо, файл получил. Напишу вам как закончу анализ.")
    except Exception as e: 
        await update.message.reply_text(f"Возникла ошибка при получения файла. Принимаю только файлы с историей roboforex.com в Excel формате. Ошибка: {str(e)}")


    # logging.warn(update.message)

    # new_file = await update.message.effective_attachment.get_file()
    # model.get_file(new_file)

    # Acknowledge file received

    # # Send the file
    # chat_id = update.message.chat.id
    # file_id = '20221222-.pptx'
    # await context.bot.send_document(chat_id=chat_id, document=file_id)


if __name__ == '__main__':
    application = ApplicationBuilder().token(config.TELEGRAM_API).build()
    
    start_handler = CommandHandler('start', start)
    application.add_handler(start_handler)
    application.add_handler(MessageHandler(filters.ALL, downloader))
    
    application.run_polling()