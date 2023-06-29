import sys
import logging
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import filters, ApplicationBuilder, ContextTypes, CommandHandler, MessageHandler
from telegram.ext import Application, CallbackQueryHandler, CommandHandler


import config
import model


logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)]
)


async def downloader(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message.chat.type == 'group':
        await update.message.reply_text("Отвечаю только в чатах один-на-один")
        return

    try:
        await model.get_file_from_message(update.message, context)
        await update.message.reply_text("Спасибо, файл получил. Отвечу вам сразу как загружу из него данные.")
    except Exception as e:
        await update.message.reply_text(f"Возникла ошибка при получения файла. Принимаю только файлы с историей roboforex.com в Excel формате. Ошибка: {str(e)}")


async def text_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message.chat.type == 'group':
        await update.message.reply_text("Отвечаю только в чатах один-на-один")
        return

    await start(update, context)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    context.job_queue.run_once(model.get_tech_data_stat,
                               when=0,
                               chat_id=update.message.chat_id,
                               data={'USER_ID': update.message.from_user.id})


async def summary(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    context.job_queue.run_once(model.get_summary,
                               when=0,
                               chat_id=update.message.chat_id,
                               data={'USER_ID': update.message.from_user.id,
                                     'INTERVAL': 'summary'})


async def week(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    context.job_queue.run_once(model.get_summary,
                               when=0,
                               chat_id=update.message.chat_id,
                               data={'USER_ID': update.message.from_user.id,
                                     'INTERVAL': 'week'})


async def weekprev(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    context.job_queue.run_once(model.get_summary,
                               when=0,
                               chat_id=update.message.chat_id,
                               data={'USER_ID': update.message.from_user.id,
                                     'INTERVAL': 'weekprev'})


async def month(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    context.job_queue.run_once(model.get_summary,
                               when=0,
                               chat_id=update.message.chat_id,
                               data={'USER_ID': update.message.from_user.id,
                                     'INTERVAL': 'month'})


async def monthprev(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    context.job_queue.run_once(model.get_summary,
                               when=0,
                               chat_id=update.message.chat_id,
                               data={'USER_ID': update.message.from_user.id,
                                     'INTERVAL': 'monthprev'})


if __name__ == '__main__':
    application = ApplicationBuilder().token(config.TELEGRAM_API).build()

    application.add_handler(CommandHandler('start', start))
    application.add_handler(CommandHandler('summary', summary))
    application.add_handler(CommandHandler('week', week))
    application.add_handler(CommandHandler('weekprev', weekprev))
    application.add_handler(CommandHandler('month', month))
    application.add_handler(CommandHandler('monthprev', monthprev))
    application.add_handler(MessageHandler(
        filters.Document.ALL, downloader))
    application.add_handler(MessageHandler(
        ~filters.Document.ALL, text_message))

    application.run_polling()
