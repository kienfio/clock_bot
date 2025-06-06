from flask import Flask, request
from telegram import Bot, Update
from telegram.ext import (
    Dispatcher, CommandHandler, MessageHandler, Filters, ConversationHandler, CallbackQueryHandler
)
import os
import logging
import traceback
from dotenv import load_dotenv
from pathlib import Path
import atexit
from bot_functions import (
    init_db, init_bot, start, clockin, clockout, offday, ot, cancel,
    error_handler, salary_start, claim_start, paid_start, pdf_start,
    viewclaims_start, checkstate_start, handle_location, db_pool,
    close_all_db_connections
)

# === 初始化设置 ===
app = Flask(__name__)

# 加载环境变量
env_path = Path('.') / '.env'
load_dotenv(dotenv_path=env_path)

TOKEN = os.getenv("TOKEN")
ADMIN_IDS = list(map(int, os.getenv("ADMIN_IDS", "1165249082").split(",")))
DEFAULT_HOURLY_RATE = float(os.getenv("DEFAULT_HOURLY_RATE", "20.00"))
DEFAULT_MONTHLY_SALARY = float(os.getenv("DEFAULT_MONTHLY_SALARY", "3500.00"))
WORKING_DAYS_PER_MONTH = int(os.getenv("WORKING_DAYS_PER_MONTH", "22"))
WORKING_HOURS_PER_DAY = int(os.getenv("WORKING_HOURS_PER_DAY", "8"))
GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')

# === 日志设置 ===
logging.basicConfig(
    level=os.getenv('LOG_LEVEL', 'INFO'),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# === Telegram Bot 设置 ===
bot = Bot(token=TOKEN)
dispatcher = None

# === 数据库连接池 ===
db_pool = None

# 确保在应用退出时关闭所有数据库连接
atexit.register(close_all_db_connections)

# === Webhook ===
@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        global db_pool
        if not db_pool:
            init_db()
        if not dispatcher:
            init_bot()
        update = Update.de_json(request.get_json(force=True), bot)
        dispatcher.process_update(update)
        return "ok"
    except Exception as e:
        logger.error(f"Error processing webhook: {e}")
        return "error", 500
    finally:
        # 确保每个请求结束后释放所有空闲连接
        if db_pool:
            try:
                while db_pool.putconn(db_pool.getconn(), close=True):
                    pass
            except psycopg2.pool.PoolError:
                pass

# === 健康检查端点 ===
@app.route("/health")
def health():
    return "OK", 200

# === 启动应用 ===
if __name__ == "__main__":
    # 本地开发时使用
    init_bot()  # 初始化 bot
    logger.info("Starting bot in development mode...")
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
else:
    # Gunicorn 生产环境使用
    logger.info("Starting bot in production mode...")
    try:
        # 获取应用URL
        render_external_url = os.environ.get("RENDER_EXTERNAL_URL")
        if not render_external_url:
            logger.warning("RENDER_EXTERNAL_URL not found, trying to get RENDER_EXTERNAL_HOSTNAME")
            render_external_url = os.environ.get("RENDER_EXTERNAL_HOSTNAME")
        
        if render_external_url:
            # 移除任何可能的 http:// 或 https:// 前缀
            render_external_url = render_external_url.replace("http://", "").replace("https://", "")
            # 构建完整的 webhook URL
            webhook_url = f"https://{render_external_url}/webhook"
            
            logger.info(f"Attempting to set webhook URL to: {webhook_url}")
            
            # 先删除现有的 webhook
            bot.delete_webhook()
            
            # 设置新的 webhook，使用最基本的配置
            success = bot.set_webhook(
                url=webhook_url,
                max_connections=100
            )
            
            if success:
                logger.info("Webhook set successfully!")
            else:
                logger.error("Failed to set webhook")
                raise ValueError("Webhook setup failed")
                
        else:
            logger.error("No valid external URL found")
            raise ValueError("No valid external URL environment variable found")
            
    except Exception as e:
        logger.error(f"Error during webhook setup: {str(e)}")
        logger.error(f"Full error: {traceback.format_exc()}")
        raise

# 添加一个路由来显示当前 webhook 状态
@app.route("/webhook-status")
def webhook_status():
    try:
        webhook_info = bot.get_webhook_info()
        return {
            "url": webhook_info.url,
            "has_custom_certificate": webhook_info.has_custom_certificate,
            "pending_update_count": webhook_info.pending_update_count,
            "last_error_date": webhook_info.last_error_date,
            "last_error_message": webhook_info.last_error_message,
            "max_connections": webhook_info.max_connections,
            "ip_address": webhook_info.ip_address
        }
    except Exception as e:
        return {"error": str(e)}
