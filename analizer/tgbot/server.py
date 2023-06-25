import logging
from telegram import Update
from telegram.ext import filters, ApplicationBuilder, ContextTypes, CommandHandler, MessageHandler

import config

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    answer = 'Send me your roboforex.com Excel history export to analyze'
    await context.bot.send_message(chat_id=update.effective_chat.id, 
                                  text=answer)

async def downloader(update, context):
    fileName = update.message.document.file_name

    logging.warn(fileName)

    new_file = await update.message.effective_attachment.get_file()
    await new_file.download_to_drive(custom_path='files/file.1')

    # Acknowledge file received
    await update.message.reply_text(f"{fileName} saved successfully")

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