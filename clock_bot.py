from flask import Flask, request
from telegram import (
    Bot, Update, ReplyKeyboardMarkup, ReplyKeyboardRemove, InlineKeyboardButton, InlineKeyboardMarkup, KeyboardButton
)
from telegram.ext import (
    Dispatcher, CommandHandler, MessageHandler, Filters, ConversationHandler, CallbackQueryHandler
)
import datetime
import pytz
import os
import logging
import traceback
import tempfile
import requests
import calendar
import psycopg2
import psycopg2.extras
from psycopg2 import pool
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from dotenv import load_dotenv
from pathlib import Path
import time
import atexit

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

# 修改：使用 UTC 作为默认时区
DEFAULT_TIMEZONE = 'UTC'

# === 日志设置 ===
logging.basicConfig(
    level=os.getenv('LOG_LEVEL', 'INFO'),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# === Telegram Bot 设置 ===
bot = Bot(token=TOKEN)
dispatcher = None

# === 状态常量 ===
SALARY_SELECT_DRIVER = 0
SALARY_ENTER_AMOUNT = 1
TOPUP_USER = 0
TOPUP_AMOUNT = 1
CLAIM_TYPE = 0
CLAIM_OTHER_TYPE = 1
CLAIM_AMOUNT = 2
CLAIM_PROOF = 3
PAID_SELECT_DRIVER = 0
PAID_START_DATE = 1
PAID_END_DATE = 2

# === 数据库连接池 ===
db_pool = None

# === 数据库工具函数 ===
def get_db_connection():
    """获取数据库连接"""
    try:
        conn = db_pool.getconn()
        return conn
    except psycopg2.pool.PoolError:
        logger.error("Connection pool exhausted, waiting for available connection...")
        # 等待一会儿再试
        time.sleep(1)
        try:
            conn = db_pool.getconn()
            return conn
        except Exception as e:
            logger.error(f"Failed to get database connection: {e}")
            raise

def release_db_connection(conn):
    """释放数据库连接回连接池"""
    try:
        if conn:
            db_pool.putconn(conn)
    except Exception as e:
        logger.error(f"Error releasing database connection: {e}")

def close_all_db_connections():
    """关闭所有数据库连接"""
    try:
        if db_pool:
            db_pool.closeall()
            logger.info("All database connections closed")
    except Exception as e:
        logger.error(f"Error closing database connections: {e}")

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

def init_db():
    """初始化数据库和表结构"""
    global db_pool
    try:
        # 创建数据库连接池，针对 Neon Database 的特定配置
        db_params = {
            'dsn': os.environ.get("DATABASE_URL"),
            'minconn': 1,
            'maxconn': 20,
            'options': "-c timezone=Asia/Kuala_Lumpur"
        }
        
        # 添加 SSL 配置
        if 'sslmode=require' in os.environ.get("DATABASE_URL", ""):
            db_params['sslmode'] = 'require'
        
        db_pool = psycopg2.pool.SimpleConnectionPool(**db_params)
        logger.info("Database connection pool created successfully")
        
        conn = get_db_connection()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                # 设置会话级别的时区
                cur.execute("SET timezone TO 'Asia/Kuala_Lumpur'")
                
                # 创建司机表
                cur.execute("""
                CREATE TABLE IF NOT EXISTS drivers (
                    user_id BIGINT PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    balance FLOAT DEFAULT 0.0,
                    monthly_salary FLOAT DEFAULT 3500.0,
                    total_hours FLOAT DEFAULT 0.0,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                )
                """)
                
                # 打卡记录表
                cur.execute("""
                CREATE TABLE IF NOT EXISTS clock_logs (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT REFERENCES drivers(user_id),
                    date DATE NOT NULL,
                    clock_in VARCHAR(30),
                    clock_out VARCHAR(30),
                    is_off BOOLEAN DEFAULT FALSE,
                    location_address TEXT,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(user_id, date)
                )
                """)
                
                # 确保 location_address 列存在
                cur.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 
                        FROM information_schema.columns 
                        WHERE table_name='clock_logs' AND column_name='location_address'
                    ) THEN
                        ALTER TABLE clock_logs ADD COLUMN location_address TEXT;
                    END IF;
                END $$;
                """)
                
                conn.commit()
                logger.info("Database tables created successfully")
        finally:
            release_db_connection(conn)
            
    except Exception as e:
        logger.error(f"Database initialization failed: {str(e)}")
        raise

def clockout(update, context):
    user = update.effective_user
    now = get_current_time()
    today = now.date()
    clock_time = now.strftime("%Y-%m-%d %H:%M:%S")
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # 检查是否已打卡
            cur.execute(
                "SELECT clock_in FROM clock_logs WHERE user_id = %s AND date = %s",
                (user.id, today)
            )
            log = cur.fetchone()
            
            if not log or not log[0] or log[0] == "OFF":
                update.message.reply_text("❌ You haven't clocked in today.")
                return
            
            # 更新打卡时间 
            cur.execute(
                "UPDATE clock_logs SET clock_out = %s WHERE user_id = %s AND date = %s",
                (clock_time, user.id, today)
            )
            
            # 处理不同格式的时间戳
            in_time = log[0]
            if isinstance(in_time, str):
                # 如果是字符串格式
                try:
                    in_time = datetime.datetime.strptime(in_time, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    # 如果解析失败，可能是其他格式，记录错误并通知用户
                    logger.error(f"Failed to parse clock_in time: {in_time}")
                    update.message.reply_text("❌ Error processing clock-in time. Please contact admin.")
                    return
            else:
                # 如果是 datetime 对象，确保它是 naive datetime
                if in_time.tzinfo:
                    in_time = in_time.replace(tzinfo=None)
            
            # 确保 out_time 也是 naive datetime
            out_time = datetime.datetime.strptime(clock_time, "%Y-%m-%d %H:%M:%S")
            hours_worked = (out_time - in_time).total_seconds() / 3600
            
            # 更新总工时
            cur.execute(
                "UPDATE drivers SET total_hours = total_hours + %s WHERE user_id = %s",
                (hours_worked, user.id)
            )
            conn.commit()
    except Exception as e:
        logger.error(f"Error in clockout: {str(e)}")
        update.message.reply_text("❌ An error occurred. Please try again or contact admin.")
        return
    finally:
        release_db_connection(conn)
    
    time_str = format_duration(hours_worked)
    update.message.reply_text(
        f"🏁 Clocked out at {format_local_time(clock_time)}. Worked {time_str}."
    )

def request_location(update, context):
    """请求用户位置"""
    keyboard = [[KeyboardButton(text="📍 Share Location", request_location=True)]]
    reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True)
    update.message.reply_text(
        "Please share your location to clock in.",
        reply_markup=reply_markup
    )
    return "WAITING_LOCATION"

def handle_location(update, context):
    """处理用户发送的位置信息"""
    user = update.effective_user
    location = update.message.location
    
    try:
        # 获取地址
        address = get_address_from_location(location.latitude, location.longitude)
        if address in ["API key not available", "Address not available", "Address lookup failed"]:
            update.message.reply_text(
                "❌ Could not get location details. Please contact admin.",
                reply_markup=ReplyKeyboardRemove()
            )
            return ConversationHandler.END
        
        # 记录打卡
        now = datetime.datetime.now(pytz.timezone('Asia/Kuala_Lumpur'))
        today = now.date()
        clock_time = now.strftime("%Y-%m-%d %H:%M:%S")
        
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                # 直接插入或更新打卡记录，不检查之前的记录
                cur.execute(
                    """INSERT INTO clock_logs 
                       (user_id, date, clock_in, location_address) 
                       VALUES (%s, %s, %s, %s)
                       ON CONFLICT (user_id, date) 
                       DO UPDATE SET 
                       clock_in = %s,
                       location_address = %s,
                       is_off = FALSE""",
                    (user.id, today, clock_time, address, clock_time, address)
                )
                conn.commit()
                
                # 发送成功消息
                local_time = now.strftime("%Y-%m-%d %H:%M")
                update.message.reply_text(
                    f"✅ Clocked in at {local_time}\n📍 Location: {address}",
                    reply_markup=ReplyKeyboardRemove()
                )
                return ConversationHandler.END
                
        except Exception as e:
            logger.error(f"Error in handle_location: {str(e)}")
            update.message.reply_text(
                "❌ An error occurred. Please try again or contact admin.",
                reply_markup=ReplyKeyboardRemove()
            )
            return ConversationHandler.END
        finally:
            release_db_connection(conn)
            
    except Exception as e:
        logger.error(f"Error processing location: {str(e)}")
        update.message.reply_text(
            "❌ An error occurred while processing your location.",
            reply_markup=ReplyKeyboardRemove()
        )
        return ConversationHandler.END

def clockin(update, context):
    """启动打卡流程"""
    return request_location(update, context)

def init_bot():
    """初始化 Telegram Bot 和 Dispatcher"""
    global dispatcher
    dispatcher = Dispatcher(bot, None, use_context=True)
    
    # 注册命令处理器
    dispatcher.add_handler(CommandHandler("start", start))
    dispatcher.add_handler(ConversationHandler(
        entry_points=[CommandHandler("clockin", clockin)],
        states={
            "WAITING_LOCATION": [MessageHandler(Filters.location, handle_location)]
        },
        fallbacks=[CommandHandler("cancel", cancel)]
    ))
    dispatcher.add_handler(CommandHandler("clockout", clockout))
    dispatcher.add_handler(CommandHandler("offday", offday))
    dispatcher.add_handler(CommandHandler("balance", balance))
    dispatcher.add_handler(CommandHandler("check", check))
    dispatcher.add_handler(CommandHandler("viewclaims", viewclaims))
    dispatcher.add_handler(CommandHandler("PDF", pdf_start))
    dispatcher.add_handler(CallbackQueryHandler(pdf_button_callback, pattern=r'^all|\d+$'))

    # 注册对话处理器
    dispatcher.add_handler(ConversationHandler(
        entry_points=[CommandHandler("salary", salary_start)],
        states={
            SALARY_SELECT_DRIVER: [MessageHandler(Filters.text & ~Filters.command, salary_select_driver)],
            SALARY_ENTER_AMOUNT: [MessageHandler(Filters.text & ~Filters.command, salary_enter_amount)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    ))

    dispatcher.add_handler(ConversationHandler(
        entry_points=[CommandHandler("topup", topup_start)],
        states={
            TOPUP_USER: [MessageHandler(Filters.text & ~Filters.command, topup_user)],
            TOPUP_AMOUNT: [MessageHandler(Filters.text & ~Filters.command, topup_amount)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    ))

    dispatcher.add_handler(ConversationHandler(
        entry_points=[CommandHandler("claim", claim_start)],
        states={
            CLAIM_TYPE: [MessageHandler(Filters.text & ~Filters.command, claim_type)],
            CLAIM_OTHER_TYPE: [MessageHandler(Filters.text & ~Filters.command, claim_other_type)],
            CLAIM_AMOUNT: [MessageHandler(Filters.text & ~Filters.command, claim_amount)],
            CLAIM_PROOF: [MessageHandler(Filters.photo, claim_proof)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    ))

    dispatcher.add_handler(ConversationHandler(
        entry_points=[CommandHandler("paid", paid_start)],
        states={
            PAID_SELECT_DRIVER: [MessageHandler(Filters.text & ~Filters.command, paid_select_driver)],
            PAID_START_DATE: [MessageHandler(Filters.text & ~Filters.command, paid_start_date)],
            PAID_END_DATE: [MessageHandler(Filters.text & ~Filters.command, paid_end_date)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    ))

    # 注册错误处理器
    dispatcher.add_error_handler(error_handler)
    
    logger.info("Bot handlers initialized successfully") 

def start(update, context):
    """处理 /start 命令"""
    user = update.effective_user
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # 检查用户是否已存在
            cur.execute("SELECT * FROM drivers WHERE user_id = %s", (user.id,))
            driver = cur.fetchone()
            
            if not driver:
                # 创建新用户
                cur.execute(
                    """INSERT INTO drivers (user_id, username, first_name) 
                       VALUES (%s, %s, %s)""",
                    (user.id, user.username, user.first_name)
                )
                conn.commit()
                welcome_msg = (
                    f"👋 Hello {user.first_name}!\n"
                    "Welcome to Worker ClockIn Bot.\n\n"
                    "Available Commands:\n"
                    "🕑 /clockin\n"
                    "🏁 /clockout\n"
                    "📅 /offday\n"
                    "💸 /claim\n\n"
                    "🔐 Admin Commands:\n"
                    "📊 /balance\n"
                    "📄 /check\n"
                    "🧾 /PDF\n"
                    "💵 /topup\n"
                    "📷 /viewclaims\n"
                    "💰 /salary\n"
                    "🟢 /paid"
                )
            else:
                welcome_msg = (
                    f"👋 Hello {user.first_name}!\n"
                    "Welcome to Worker ClockIn Bot.\n\n"
                    "Available Commands:\n"
                    "🕑 /clockin\n"
                    "🏁 /clockout\n"
                    "📅 /offday\n"
                    "💸 /claim\n\n"
                    "🔐 Admin Commands:\n"
                    "📊 /balance\n"
                    "📄 /check\n"
                    "🧾 /PDF\n"
                    "💵 /topup\n"
                    "📷 /viewclaims\n"
                    "💰 /salary\n"
                    "🟢 /paid"
                )
    except Exception as e:
        logger.error(f"Error in start command: {str(e)}")
        welcome_msg = "❌ An error occurred. Please try again or contact admin."
    finally:
        release_db_connection(conn)
    
    update.message.reply_text(welcome_msg)

def check(update, context):
    """检查今天的打卡记录"""
    user = update.effective_user
    now = datetime.datetime.now(pytz.timezone('Asia/Kuala_Lumpur'))
    today = now.date()
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT clock_in, clock_out, is_off, location_address 
                   FROM clock_logs 
                   WHERE user_id = %s AND date = %s""",
                (user.id, today)
            )
            log = cur.fetchone()
            
            if not log:
                update.message.reply_text("📝 No records for today.")
                return
            
            clock_in, clock_out, is_off, location = log
            
            if is_off:
                update.message.reply_text("🏖 Today is marked as off day.")
                return
            
            status = []
            if clock_in and clock_in != "OFF":
                status.append(f"Clock in: {clock_in}")
                if location:
                    status.append(f"📍 Location: {location}")
            if clock_out:
                status.append(f"Clock out: {clock_out}")
            
            if status:
                update.message.reply_text("\n".join(["📝 Today's Record:"] + status))
            else:
                update.message.reply_text("📝 No clock in/out records for today.")
                
    except Exception as e:
        logger.error(f"Error in check command: {str(e)}")
        update.message.reply_text("❌ An error occurred. Please try again or contact admin.")
    finally:
        release_db_connection(conn)

def offday(update, context):
    """标记今天为休息日"""
    user = update.effective_user
    now = datetime.datetime.now(pytz.timezone('Asia/Kuala_Lumpur'))
    today = now.date()
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # 检查是否已有记录
            cur.execute(
                "SELECT clock_in, clock_out FROM clock_logs WHERE user_id = %s AND date = %s",
                (user.id, today)
            )
            log = cur.fetchone()
            
            if log and (log[0] not in [None, "OFF"] or log[1]):
                update.message.reply_text("❌ Cannot mark as off day - already have clock records for today.")
                return
            
            # 更新或插入休息日记录
            if log:
                cur.execute(
                    """UPDATE clock_logs 
                       SET clock_in = 'OFF', clock_out = NULL, is_off = TRUE 
                       WHERE user_id = %s AND date = %s""",
                    (user.id, today)
                )
            else:
                cur.execute(
                    """INSERT INTO clock_logs (user_id, date, clock_in, is_off) 
                       VALUES (%s, %s, 'OFF', TRUE)""",
                    (user.id, today)
                )
            conn.commit()
            
            update.message.reply_text("🏖 Today has been marked as off day.")
            
    except Exception as e:
        logger.error(f"Error in offday command: {str(e)}")
        update.message.reply_text("❌ An error occurred. Please try again or contact admin.")
    finally:
        release_db_connection(conn)

def cancel(update, context):
    """取消当前操作"""
    update.message.reply_text(
        "Operation cancelled.",
        reply_markup=ReplyKeyboardRemove()
    )
    return ConversationHandler.END

def error_handler(update, context):
    """处理错误"""
    logger.error(f"Error: {context.error}")
    try:
        if update and update.effective_message:
            update.effective_message.reply_text(
                "❌ An error occurred. Please try again or contact admin."
            )
    except Exception as e:
        logger.error(f"Error in error handler: {str(e)}")

def salary_start(update, context):
    # Implementation of salary_start function
    pass

def salary_select_driver(update, context):
    # Implementation of salary_select_driver function
    pass

def salary_enter_amount(update, context):
    # Implementation of salary_enter_amount function
    pass

def topup_start(update, context):
    # Implementation of topup_start function
    pass

def topup_user(update, context):
    # Implementation of topup_user function
    pass

def topup_amount(update, context):
    # Implementation of topup_amount function
    pass

def claim_start(update, context):
    # Implementation of claim_start function
    pass

def claim_type(update, context):
    # Implementation of claim_type function
    pass

def claim_other_type(update, context):
    # Implementation of claim_other_type function
    pass

def claim_amount(update, context):
    # Implementation of claim_amount function
    pass

def claim_proof(update, context):
    # Implementation of claim_proof function
    pass

def paid_start(update, context):
    # Implementation of paid_start function
    pass

def paid_select_driver(update, context):
    # Implementation of paid_select_driver function
    pass

def paid_start_date(update, context):
    # Implementation of paid_start_date function
    pass

def paid_end_date(update, context):
    # Implementation of paid_end_date function
    pass

def pdf_start(update, context):
    # Implementation of pdf_start function
    pass

def pdf_button_callback(update, context):
    # Implementation of pdf_button_callback function
    pass

def viewclaims(update, context):
    # Implementation of viewclaims function
    pass

def balance(update, context):
    # Implementation of balance function
    pass

def get_current_time():
    """获取当前时间（马来西亚时区）"""
    return datetime.datetime.now(pytz.timezone('Asia/Kuala_Lumpur'))

def format_duration(hours):
    """格式化工作时长"""
    hours = round(hours, 2)
    if hours == int(hours):
        return f"{int(hours)}h"
    return f"{hours}h"

def format_local_time(datetime_str):
    """格式化本地时间显示"""
    try:
        if isinstance(datetime_str, str):
            dt = datetime.datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")
        else:
            dt = datetime_str
        return dt.strftime("%Y-%m-%d %H:%M")
    except Exception as e:
        logger.error(f"Error formatting time: {str(e)}")
        return datetime_str

def get_address_from_location(latitude, longitude):
    """根据经纬度获取地址"""
    try:
        GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY")
        if not GOOGLE_API_KEY:
            logger.error("GOOGLE_API_KEY not set in environment variables")
            return "Location details not available"
            
        url = f"https://maps.googleapis.com/maps/api/geocode/json?latlng={latitude},{longitude}&key={GOOGLE_API_KEY}"
        response = requests.get(url, timeout=5)
        data = response.json()
        
        if data['status'] == 'OK' and data['results']:
            return data['results'][0]['formatted_address']
        else:
            logger.error(f"Error getting address: {data}")
            return "Address not available"
    except Exception as e:
        logger.error(f"Error in get_address_from_location: {str(e)}")
        return "Address lookup failed" 
