import logging
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import filters, ApplicationBuilder, ContextTypes, CommandHandler, MessageHandler
from telegram.ext import Application, CallbackQueryHandler, CommandHandler


import config
import model


logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):

    keyboard = [
        [InlineKeyboardButton("Инструкция: Как пользоваться?", callback_data='howto')],
        [InlineKeyboardButton("Скажи какой период уже загружен?", callback_data='stat')],
        [InlineKeyboardButton("Покажи отчеты", callback_data='reports')],
    ]

    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text("Please choose:", reply_markup=reply_markup)    


    # answer = 'Send me your roboforex.com Excel history export to analyze'
    # await context.bot.send_message(chat_id=update.effective_chat.id, 
    #                                text=answer)

async def button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Parses the CallbackQuery and updates the message text."""
    query = update.callback_query

    # CallbackQueries need to be answered, even if no notification to the user is needed
    # Some clients may have trouble otherwise. See https://core.telegram.org/bots/api#callbackquery
    await query.answer()

    if query.data == 'stat':
        # print(f'{query.message.chat_id=}')
        # print(f'{query.message.from_user.id=}')
        await stat(query, context)
    elif query.data == 'reports':
        await query.edit_message_text(text=f"Selected option: {query.data}")
    else:
        await query.edit_message_text(text=f"Selected option: {query.data}")


async def downloader(update, context):
    # logging.warn(from_user)
    # print(update.message)

    try:
        await model.get_file_from_message(update.message, context)    
        await update.message.reply_text("Спасибо, файл получил, напишу вам как обработаю")
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


async def stat(update, context):
    # await model.get_tech_data_stat(context=context)
    # answer = 'Send me your roboforex.com Excel history export to analyze'
    # await context.bot.send_message(chat_id=update.effective_chat.id, 
    #                                text=answer)

    context.job_queue.run_once(model.get_tech_data_stat, 
                            when=0, 
                            chat_id=update.message.chat_id,
                            data={'USER_ID': update.message.from_user.id})     


async def summary(update, context):
    context.job_queue.run_once(model.get_summary, 
                            when=0, 
                            chat_id=update.message.chat_id,
                            data={'USER_ID': update.message.from_user.id,
                                  'INTERVAL': 'summary'})


async def week(update, context):
    context.job_queue.run_once(model.get_summary, 
                            when=0, 
                            chat_id=update.message.chat_id,
                            data={'USER_ID': update.message.from_user.id,
                                  'INTERVAL': 'week'})


async def weekprev(update, context):
    context.job_queue.run_once(model.get_summary, 
                            when=0, 
                            chat_id=update.message.chat_id,
                            data={'USER_ID': update.message.from_user.id,
                                  'INTERVAL': 'weekprev'})
    

async def month(update, context):
    context.job_queue.run_once(model.get_summary, 
                            when=0, 
                            chat_id=update.message.chat_id,
                            data={'USER_ID': update.message.from_user.id,
                                  'INTERVAL': 'month'})
    

async def monthprev(update, context):
    context.job_queue.run_once(model.get_summary, 
                            when=0, 
                            chat_id=update.message.chat_id,
                            data={'USER_ID': update.message.from_user.id,
                                  'INTERVAL': 'monthprev'})
        

if __name__ == '__main__':
    application = ApplicationBuilder().token(config.TELEGRAM_API).build()
    
    application.add_handler(CommandHandler('start', start))
    application.add_handler(CommandHandler('stat', stat))
    application.add_handler(CommandHandler('summary', summary))
    application.add_handler(CommandHandler('week', week))
    application.add_handler(CommandHandler('weekprev', weekprev))
    application.add_handler(CommandHandler('month', month))
    application.add_handler(CommandHandler('monthprev', monthprev))
    application.add_handler(MessageHandler(filters.ALL, downloader))

    # application.add_handler(CallbackQueryHandler(button))
    
    application.run_polling()


