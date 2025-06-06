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
SALARY_CONFIRM = 2
CLAIM_TYPE = 0
CLAIM_OTHER_TYPE = 1
CLAIM_AMOUNT = 2
CLAIM_PROOF = 3
PAID_SELECT_DRIVER = 0
PAID_CONFIRM = 1
VIEWCLAIMS_SELECT_USER = 10
CHECKSTATE_SELECT_USER = 11

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
                    monthly_salary FLOAT DEFAULT 0.0,
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
                
                # 添加 OT 记录表
                cur.execute("""
                CREATE TABLE IF NOT EXISTS ot_logs (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT REFERENCES drivers(user_id),
                    date DATE NOT NULL,
                    start_time TIMESTAMP WITH TIME ZONE,
                    end_time TIMESTAMP WITH TIME ZONE,
                    duration FLOAT DEFAULT 0.0,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                )
                """)
                
                # 添加工资发放记录表
                cur.execute("""
                CREATE TABLE IF NOT EXISTS salary_payments (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT REFERENCES drivers(user_id),
                    payment_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    salary_amount FLOAT NOT NULL,
                    claims_amount FLOAT DEFAULT 0.0,
                    total_amount FLOAT NOT NULL,
                    work_days INTEGER DEFAULT 0,
                    off_days INTEGER DEFAULT 0,
                    work_hours FLOAT DEFAULT 0.0,
                    ot_hours FLOAT DEFAULT 0.0,
                    period_start DATE NOT NULL,
                    period_end DATE NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                )
                """)
                
                # 添加报销记录表
                cur.execute("""
                CREATE TABLE IF NOT EXISTS claims (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT REFERENCES drivers(user_id),
                    type TEXT NOT NULL,
                    amount FLOAT NOT NULL,
                    date DATE NOT NULL,
                    photo_file_id TEXT,
                    status TEXT DEFAULT 'PENDING',
                    paid_date TIMESTAMP WITH TIME ZONE,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                )
                """)
                
                # 确保 claims 表中的 status 和 paid_date 列存在
                cur.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 
                        FROM information_schema.columns 
                        WHERE table_name='claims' AND column_name='status'
                    ) THEN
                        ALTER TABLE claims ADD COLUMN status TEXT DEFAULT 'PENDING';
                    END IF;
                    
                    IF NOT EXISTS (
                        SELECT 1 
                        FROM information_schema.columns 
                        WHERE table_name='claims' AND column_name='paid_date'
                    ) THEN
                        ALTER TABLE claims ADD COLUMN paid_date TIMESTAMP WITH TIME ZONE;
                    END IF;
                END $$;
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
    try:
        user = update.effective_user
        logger.info(f"User {user.id} ({user.first_name}) requested clock in")
        
        # 首先确认用户存在于数据库中
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT user_id FROM drivers WHERE user_id = %s", (user.id,))
                driver = cur.fetchone()
                
                if not driver:
                    # 如果用户不存在，先创建用户
                    cur.execute(
                        """INSERT INTO drivers (user_id, username, first_name) 
                           VALUES (%s, %s, %s)""",
                        (user.id, user.username, user.first_name)
                    )
                    conn.commit()
                    logger.info(f"Created new user: {user.id} ({user.first_name})")
        finally:
            release_db_connection(conn)
        
        # 请求位置
        keyboard = [[KeyboardButton(text="📍 Share Location", request_location=True)]]
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
        update.message.reply_text(
            "Please share your location to clock in.",
            reply_markup=reply_markup
        )
        return "WAITING_LOCATION"
    except Exception as e:
        logger.error(f"Error in clockin: {str(e)}")
        update.message.reply_text("❌ An error occurred. Please try again or contact admin.")
        return ConversationHandler.END

def fix_claims_data():
    """修复 claims 表中的数据，确保所有记录都有正确的状态"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # 将所有 NULL 状态的记录更新为 'PENDING'
            cur.execute(
                """UPDATE claims 
                   SET status = 'PENDING' 
                   WHERE status IS NULL"""
            )
            rows_updated = cur.rowcount
            conn.commit()
            logger.info(f"Fixed {rows_updated} claims records with NULL status")
    except Exception as e:
        logger.error(f"Error fixing claims data: {str(e)}")
    finally:
        release_db_connection(conn)

def init_bot():
    """初始化 Telegram Bot 和 Dispatcher"""
    global dispatcher
    dispatcher = Dispatcher(bot, None, use_context=True)
    
    # 修复数据
    fix_claims_data()
    
    # 注册命令处理器
    dispatcher.add_handler(CommandHandler("start", start))
    
    # 注册对话处理器（按照优先级顺序排列）
    
    # 1. 查看状态对话处理器
    dispatcher.add_handler(ConversationHandler(
        entry_points=[CommandHandler("checkstate", checkstate_start)],
        states={
            CHECKSTATE_SELECT_USER: [MessageHandler(Filters.text & ~Filters.command, checkstate_select_user)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        allow_reentry=True
    ))
    
    # 2. 查看报销记录对话处理器
    dispatcher.add_handler(ConversationHandler(
        entry_points=[CommandHandler("viewclaims", viewclaims_start)],
        states={
            VIEWCLAIMS_SELECT_USER: [MessageHandler(Filters.text & ~Filters.command, viewclaims_select_user)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        allow_reentry=True
    ))
    
    # 3. 报销对话处理器
    dispatcher.add_handler(ConversationHandler(
        entry_points=[CommandHandler("claim", claim_start)],
        states={
            CLAIM_TYPE: [MessageHandler(Filters.text & ~Filters.command, claim_type)],
            CLAIM_OTHER_TYPE: [MessageHandler(Filters.text & ~Filters.command, claim_other_type)],
            CLAIM_AMOUNT: [MessageHandler(Filters.text & ~Filters.command, claim_amount)],
            CLAIM_PROOF: [MessageHandler(Filters.photo, claim_proof)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        allow_reentry=True
    ))
    
    # 4. 设置工资对话处理器
    dispatcher.add_handler(ConversationHandler(
        entry_points=[CommandHandler("salary", salary_start)],
        states={
            SALARY_SELECT_DRIVER: [MessageHandler(Filters.text & ~Filters.command, salary_select_driver)],
            SALARY_ENTER_AMOUNT: [MessageHandler(Filters.text & ~Filters.command, salary_enter_amount)],
            SALARY_CONFIRM: [MessageHandler(Filters.text & ~Filters.command, salary_confirm)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        allow_reentry=True
    ))
    
    # 5. 工资发放对话处理器
    dispatcher.add_handler(ConversationHandler(
        entry_points=[CommandHandler("paid", paid_start)],
        states={
            PAID_SELECT_DRIVER: [MessageHandler(Filters.text & ~Filters.command, paid_select_driver)],
            PAID_CONFIRM: [MessageHandler(Filters.text & ~Filters.command, paid_confirm)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        allow_reentry=True
    ))
    
    # 6. 打卡对话处理器
    dispatcher.add_handler(ConversationHandler(
        entry_points=[CommandHandler("clockin", clockin)],
        states={
            "WAITING_LOCATION": [MessageHandler(Filters.location, handle_location)]
        },
        fallbacks=[CommandHandler("cancel", cancel)]
    ))
    
    # PDF 生成命令和回调
    dispatcher.add_handler(CommandHandler("PDF", pdf_start))
    dispatcher.add_handler(CallbackQueryHandler(pdf_button_callback, pattern=r"^pdf_"))
    
    # 历史报告命令和回调
    dispatcher.add_handler(CommandHandler("previousreport", previousreport))
    dispatcher.add_handler(CallbackQueryHandler(show_monthly_report, pattern=r"^report_"))
    
    # 注册简单命令处理器
    dispatcher.add_handler(CommandHandler("clockout", clockout))
    dispatcher.add_handler(CommandHandler("offday", offday))
    dispatcher.add_handler(CommandHandler("OT", ot))
    
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
            
            welcome_msg = (
                f"👋 Hello {user.first_name}!\n"
                "Welcome to Worker ClockIn Bot.\n\n"
                "Available Commands:\n"
                "🕑 /clockin\n"
                "🏁 /clockout\n"
                "📅 /offday\n"
                "💸 /claim\n"
                "⏰ /OT\n\n"
                "🔐 Admin Commands:\n"
                "📊 /checkstate\n"
                "🧾 /PDF\n"
                "📷 /viewclaims\n"
                "💰 /salary\n"
                "🟢 /paid\n"
                "📋 /previousreport"
            )
            
            if not driver:
                # 创建新用户，确保工资为0
                cur.execute(
                    """INSERT INTO drivers (user_id, username, first_name, monthly_salary) 
                       VALUES (%s, %s, %s, 0.0)""",
                    (user.id, user.username, user.first_name)
                )
                conn.commit()
                
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
    """开始设置工资流程"""
    user = update.effective_user
    if user.id not in ADMIN_IDS:
        update.message.reply_text("❌ This command is only available for admins.")
        return ConversationHandler.END
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT user_id, first_name, monthly_salary FROM drivers ORDER BY first_name")
            drivers = cur.fetchall()
            
            if not drivers:
                update.message.reply_text("❌ No workers found in the system.")
                return ConversationHandler.END
            
            message = ["👨‍💼 *Select a worker to set salary:*\n"]
            keyboard = []
            for driver in drivers:
                user_id, name, salary = driver
                message.append(f"*{name}*\nID: `{user_id}`\nCurrent Salary: RM {salary:.2f}\n")
                keyboard.append([f"{name} ({user_id})"])
            
            keyboard.append(["❌ Cancel"])
            reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True)
            
            update.message.reply_text(
                "\n".join(message),
                reply_markup=reply_markup,
                parse_mode='Markdown'
            )
            return SALARY_SELECT_DRIVER
    except Exception as e:
        logger.error(f"Error in salary_start: {str(e)}")
        update.message.reply_text("❌ An error occurred. Please try again.")
        return ConversationHandler.END
    finally:
        release_db_connection(conn)

def salary_select_driver(update, context):
    """选择要设置工资的司机"""
    if update.message.text == "❌ Cancel":
        update.message.reply_text(
            "Operation cancelled.",
            reply_markup=ReplyKeyboardRemove()
        )
        return ConversationHandler.END
    
    try:
        # Extract user_id from the button text (format: "Name (user_id)")
        user_id = int(update.message.text.split("(")[-1].strip(")"))
        context.user_data['target_user_id'] = user_id
        context.user_data['worker_name'] = update.message.text.split(" (")[0]
        
        keyboard = [["❌ Cancel"]]
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True)
        
        update.message.reply_text(
            f"Setting salary for: *{context.user_data['worker_name']}*\n"
            "Please enter the new monthly salary amount (e.g., 3500.00):",
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
        return SALARY_ENTER_AMOUNT
    except (ValueError, IndexError) as e:
        logger.error(f"Error in salary_select_driver: {str(e)}")
        update.message.reply_text("❌ Please select a valid worker from the list.")
        return SALARY_SELECT_DRIVER

def salary_enter_amount(update, context):
    """设置新的工资金额"""
    if update.message.text == "❌ Cancel":
        update.message.reply_text(
            "Operation cancelled.",
            reply_markup=ReplyKeyboardRemove()
        )
        return ConversationHandler.END
        
    try:
        amount = float(update.message.text)
        if amount < 0:
            update.message.reply_text("❌ Salary amount cannot be negative.")
            return SALARY_ENTER_AMOUNT
        
        # Store amount in context for confirmation
        context.user_data['new_salary'] = amount
        
        # Create confirmation keyboard
        keyboard = [
            ["✅ Confirm"],
            ["❌ Cancel"]
        ]
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True)
        
        # Show confirmation message
        update.message.reply_text(
            f"📝 *Salary Update Summary*\n\n"
            f"Worker: *{context.user_data['worker_name']}*\n"
            f"New Salary: RM {amount:.2f}\n\n"
            "Please confirm this change:",
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
        return SALARY_CONFIRM
    except ValueError:
        update.message.reply_text(
            "❌ Please enter a valid number (e.g., 3500.00)."
        )
        return SALARY_ENTER_AMOUNT

def salary_confirm(update, context):
    """确认工资更新"""
    if update.message.text == "❌ Cancel":
        update.message.reply_text(
            "Operation cancelled.",
            reply_markup=ReplyKeyboardRemove()
        )
        return ConversationHandler.END
        
    if update.message.text != "✅ Confirm":
        update.message.reply_text("Please either confirm or cancel the operation.")
        return SALARY_CONFIRM
        
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE drivers SET monthly_salary = %s WHERE user_id = %s",
                (context.user_data['new_salary'], context.user_data['target_user_id'])
            )
            conn.commit()
            
            update.message.reply_text(
                f"✅ Salary updated successfully!\n\n"
                f"Worker: *{context.user_data['worker_name']}*\n"
                f"New Salary: RM {context.user_data['new_salary']:.2f}",
                reply_markup=ReplyKeyboardRemove(),
                parse_mode='Markdown'
            )
    except Exception as e:
        logger.error(f"Error in salary_confirm: {str(e)}")
        update.message.reply_text(
            "❌ An error occurred while updating the salary. Please try again.",
            reply_markup=ReplyKeyboardRemove()
        )
    finally:
        release_db_connection(conn)
        context.user_data.clear()
        
    return ConversationHandler.END

def claim_start(update, context):
    """开始报销流程"""
    user = update.effective_user
    keyboard = [
        ['🍱 Meal', '🚗 Transport'],
        ['🏥 Medical', '📱 Phone'],
        ['🛠 Tools', '👔 Uniform'],
        ['Other']
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True)
    update.message.reply_text(
        "Please select claim type:",
        reply_markup=reply_markup
    )
    return CLAIM_TYPE

def claim_type(update, context):
    """处理报销类型选择"""
    claim_type = update.message.text
    if claim_type == 'Other':
        update.message.reply_text(
            "Please specify the claim type:",
            reply_markup=ReplyKeyboardRemove()
        )
        return CLAIM_OTHER_TYPE
    
    context.user_data['claim_type'] = claim_type
    update.message.reply_text(
        "Please enter the claim amount (e.g., 50.00):",
        reply_markup=ReplyKeyboardRemove()
    )
    return CLAIM_AMOUNT

def claim_other_type(update, context):
    """处理其他类型的报销"""
    claim_type = update.message.text
    context.user_data['claim_type'] = claim_type
    update.message.reply_text(
        "Please enter the claim amount (e.g., 50.00):",
        reply_markup=ReplyKeyboardRemove()
    )
    return CLAIM_AMOUNT

def claim_amount(update, context):
    """处理报销金额"""
    try:
        amount = float(update.message.text)
        if amount <= 0:
            update.message.reply_text("❌ Amount must be greater than 0.")
            return CLAIM_AMOUNT
        
        context.user_data['claim_amount'] = amount
        update.message.reply_text(
            "Please send a photo of the receipt/proof:",
            reply_markup=ReplyKeyboardRemove()
        )
        return CLAIM_PROOF
    except ValueError:
        update.message.reply_text("❌ Please enter a valid number.")
        return CLAIM_AMOUNT

def claim_proof(update, context):
    """处理报销凭证"""
    user = update.effective_user
    photo = update.message.photo[-1]
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO claims (user_id, type, amount, date, photo_file_id, status)
                   VALUES (%s, %s, %s, %s, %s, %s)""",
                (user.id, 
                 context.user_data['claim_type'],
                 context.user_data['claim_amount'],
                 datetime.datetime.now(pytz.timezone('Asia/Kuala_Lumpur')).date(),
                 photo.file_id,
                 'PENDING')
            )
            conn.commit()
            
            update.message.reply_text(
                f"✅ Claim submitted:\n"
                f"Type: {context.user_data['claim_type']}\n"
                f"Amount: RM {context.user_data['claim_amount']:.2f}\n"
                "Status: Pending approval"
            )
    except Exception as e:
        logger.error(f"Error in claim_proof: {str(e)}")
        update.message.reply_text("❌ An error occurred. Please try again.")
    finally:
        release_db_connection(conn)
    
    context.user_data.clear()
    return ConversationHandler.END

def paid_start(update, context):
    """开始发放工资流程"""
    user = update.effective_user
    if user.id not in ADMIN_IDS:
        update.message.reply_text("❌ This command is only available for admins.")
        return ConversationHandler.END
    
    return show_workers_page(update, context, page=1, command="paid")

def paid_select_driver(update, context):
    """选择要发放工资的员工"""
    # 检查是否是导航命令
    nav_result = handle_page_navigation(update, context)
    if nav_result is not None:
        return nav_result
    
    if update.message.text == "❌ Cancel":
        update.message.reply_text(
            "Operation cancelled.",
            reply_markup=ReplyKeyboardRemove()
        )
        return ConversationHandler.END
    
    try:
        # 记录日志，帮助调试
        logger.info(f"paid_select_driver received text: '{update.message.text}'")
        
        user_id = int(update.message.text.split()[0])
        context.user_data['target_user_id'] = user_id
        
        # 获取本月的第一天和最后一天
        today = datetime.datetime.now(pytz.timezone('Asia/Kuala_Lumpur')).date()
        first_day = today.replace(day=1)
        # 计算下个月的第一天，然后减去一天得到本月最后一天
        if today.month == 12:
            next_month = today.replace(year=today.year + 1, month=1, day=1)
        else:
            next_month = today.replace(month=today.month + 1, day=1)
        last_day = next_month - datetime.timedelta(days=1)
        
        logger.info(f"Period: {first_day} to {last_day}")
        
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                # 获取员工基本信息
                cur.execute(
                    """SELECT first_name, monthly_salary 
                       FROM drivers 
                       WHERE user_id = %s""",
                    (user_id,)
                )
                worker_info = cur.fetchone()
                if not worker_info:
                    update.message.reply_text(
                        "❌ Worker not found.",
                        reply_markup=ReplyKeyboardRemove()
                    )
                    return ConversationHandler.END
                
                name, monthly_salary = worker_info
                context.user_data['worker_name'] = name
                
                # 获取本月工作统计
                cur.execute(
                    """SELECT 
                        COUNT(DISTINCT CASE WHEN NOT is_off THEN date END) as work_days,
                        COUNT(DISTINCT CASE WHEN is_off THEN date END) as off_days
                       FROM clock_logs 
                       WHERE user_id = %s 
                       AND date BETWEEN %s AND %s""",
                    (user_id, first_day, last_day)
                )
                work_stats = cur.fetchone()
                work_days, off_days = work_stats if work_stats else (0, 0)
                
                # 获取本月工作时长
                cur.execute(
                    """SELECT date, clock_in, clock_out, is_off
                       FROM clock_logs 
                       WHERE user_id = %s 
                       AND date BETWEEN %s AND %s""",
                    (user_id, first_day, last_day)
                )
                logs = cur.fetchall()
                
                # 计算工作时长
                month_hours = 0
                for log in logs:
                    date, clock_in, clock_out, is_off = log
                    if not is_off and clock_in and clock_out and clock_in != 'OFF' and clock_out != 'OFF':
                        try:
                            if isinstance(clock_in, str) and isinstance(clock_out, str):
                                in_time = datetime.datetime.strptime(clock_in, "%Y-%m-%d %H:%M:%S")
                                out_time = datetime.datetime.strptime(clock_out, "%Y-%m-%d %H:%M:%S")
                                hours = (out_time - in_time).total_seconds() / 3600
                                if hours > 0:
                                    month_hours += hours
                        except (ValueError, TypeError):
                            continue
                
                # 获取本月 OT 时长
                cur.execute(
                    """SELECT COALESCE(SUM(duration), 0) as total_ot_hours
                       FROM ot_logs 
                       WHERE user_id = %s 
                       AND date BETWEEN %s AND %s
                       AND end_time IS NOT NULL""",
                    (user_id, first_day, last_day)
                )
                ot_hours = cur.fetchone()[0] or 0
                ot_hours_int = int(ot_hours)
                ot_minutes = int((ot_hours - ot_hours_int) * 60)
                
                # 获取本月报销总额
                cur.execute(
                    """SELECT COALESCE(SUM(amount), 0) as total_claims
                       FROM claims 
                       WHERE user_id = %s 
                       AND date BETWEEN %s AND %s
                       AND (status IS NULL OR status = 'PENDING')""",
                    (user_id, first_day, last_day)
                )
                claims_amount = cur.fetchone()[0] or 0
                
                # 保存数据到上下文
                context.user_data.update({
                    'monthly_salary': monthly_salary,
                    'work_days': work_days,
                    'off_days': off_days,
                    'month_hours': month_hours,
                    'ot_hours': ot_hours,
                    'claims_amount': claims_amount,
                    'first_day': first_day,
                    'last_day': last_day
                })
                
                # 创建工资总结消息
                message = [
                    f"💰 Salary Summary for {name}\n",
                    f"📅 Period: {first_day.strftime('%Y-%m-%d')} to {last_day.strftime('%Y-%m-%d')}\n",
                    f"💵 Base Salary: RM {monthly_salary:.2f}",
                    f"⏰ Work Hours: {format_duration(month_hours)}",
                    f"🕒 OT Hours: {ot_hours_int}h {ot_minutes}m",
                    f"📊 Work Days: {work_days} days",
                    f"🏖 Off Days: {off_days} days",
                    f"🧾 Claims: RM {claims_amount:.2f}\n",
                    "Do you want to mark this month's salary as paid?"
                ]
                
                # 创建确认键盘
                keyboard = [
                    ["✅ Confirm Payment"],
                    ["❌ Cancel"]
                ]
                reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True)
                
                update.message.reply_text(
                    "\n".join(message),
                    reply_markup=reply_markup
                )
                return PAID_CONFIRM
                
        except Exception as e:
            logger.error(f"Error in paid_select_driver: {str(e)}")
            update.message.reply_text(
                "❌ An error occurred. Please try again.",
                reply_markup=ReplyKeyboardRemove()
            )
            return ConversationHandler.END
        finally:
            release_db_connection(conn)
            
    except (ValueError, IndexError) as e:
        logger.error(f"Error parsing user input in paid_select_driver: {str(e)}")
        update.message.reply_text(
            "❌ Please select a valid worker.",
            reply_markup=ReplyKeyboardRemove()
        )
        return PAID_SELECT_DRIVER

def paid_confirm(update, context):
    """确认工资发放"""
    if update.message.text == "❌ Cancel":
        update.message.reply_text(
            "Operation cancelled.",
            reply_markup=ReplyKeyboardRemove()
        )
        return ConversationHandler.END
    
    if update.message.text != "✅ Confirm Payment":
        update.message.reply_text("Please either confirm or cancel the operation.")
        return PAID_CONFIRM
    
    user_id = context.user_data['target_user_id']
    name = context.user_data['worker_name']
    monthly_salary = context.user_data['monthly_salary']
    claims_amount = context.user_data['claims_amount']
    first_day = context.user_data['first_day']
    last_day = context.user_data['last_day']
    
    # 计算总金额
    total_amount = monthly_salary + claims_amount
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # 记录工资发放
            cur.execute(
                """INSERT INTO salary_payments 
                   (user_id, payment_date, salary_amount, claims_amount, total_amount, 
                    work_days, off_days, work_hours, ot_hours, period_start, period_end)
                   VALUES (%s, CURRENT_TIMESTAMP, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (user_id, monthly_salary, claims_amount, total_amount,
                 context.user_data['work_days'], context.user_data['off_days'],
                 context.user_data['month_hours'], context.user_data['ot_hours'],
                 first_day, last_day)
            )
            
            # 保存月度报告
            cur.execute(
                """INSERT INTO monthly_reports 
                   (user_id, report_date, total_claims, total_ot_hours, total_salary, work_days)
                   VALUES (%s, %s, %s, %s, %s, %s)
                   ON CONFLICT (user_id, report_date) 
                   DO UPDATE SET
                       total_claims = EXCLUDED.total_claims,
                       total_ot_hours = EXCLUDED.total_ot_hours,
                       total_salary = EXCLUDED.total_salary,
                       work_days = EXCLUDED.work_days""",
                (user_id, first_day, claims_amount, context.user_data['ot_hours'],
                 monthly_salary, context.user_data['work_days'])
            )
            
            # 清除已支付的报销记录
            cur.execute(
                """UPDATE claims 
                   SET status = 'PAID', paid_date = CURRENT_TIMESTAMP
                   WHERE user_id = %s 
                   AND date BETWEEN %s AND %s 
                   AND (status IS NULL OR status = 'PENDING')""",
                (user_id, first_day, last_day)
            )
            
            conn.commit()
            
            # 发送确认消息
            message = [
                f"✅ Payment Confirmed for {name}\n",
                f"💵 Base Salary: RM {monthly_salary:.2f}",
                f"🧾 Claims: RM {claims_amount:.2f}",
                f"💰 Total Paid: RM {total_amount:.2f}\n",
                f"Payment recorded successfully!"
            ]
            
            update.message.reply_text(
                "\n".join(message),
                reply_markup=ReplyKeyboardRemove()
            )
            
    except Exception as e:
        logger.error(f"Error in paid_confirm: {str(e)}")
        update.message.reply_text(
            "❌ An error occurred while processing the payment. Please try again.",
            reply_markup=ReplyKeyboardRemove()
        )
    finally:
        release_db_connection(conn)
        context.user_data.clear()
    
    return ConversationHandler.END

def pdf_start(update, context):
    """开始生成PDF报告流程"""
    user = update.effective_user
    if user.id not in ADMIN_IDS:
        update.message.reply_text("❌ This command is only available for admins.")
        return ConversationHandler.END
    
    # 创建内联键盘，提供不同类型的报告选项
    keyboard = [
        [InlineKeyboardButton("📊 工作时间报告", callback_data="pdf_work_hours")],
        [InlineKeyboardButton("💰 薪资报告", callback_data="pdf_salary")],
        [InlineKeyboardButton("🧾 全部数据报告", callback_data="pdf_all")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    update.message.reply_text(
        "请选择要生成的报告类型:",
        reply_markup=reply_markup
    )
    return ConversationHandler.END

def pdf_button_callback(update, context):
    """处理PDF报告选择按钮回调"""
    query = update.callback_query
    query.answer()
    
    report_type = query.data.replace("pdf_", "")
    user = query.from_user
    
    if user.id not in ADMIN_IDS:
        query.edit_message_text("❌ 只有管理员才能生成报告。")
        return
    
    query.edit_message_text("🔄 正在生成报告，请稍候...")
    
    try:
        # 获取本月的第一天和最后一天
        today = datetime.datetime.now(pytz.timezone('Asia/Kuala_Lumpur')).date()
        first_day = today.replace(day=1)
        next_month = today.replace(day=28) + datetime.timedelta(days=4)
        last_day = (next_month - datetime.timedelta(days=next_month.day)).date()
        
        conn = get_db_connection()
        try:
            # 生成PDF文件
            pdf_file = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
            pdf_path = pdf_file.name
            pdf_file.close()
            
            doc = SimpleDocTemplate(pdf_path, pagesize=A4)
            elements = []
            
            # 添加标题
            styles = getSampleStyleSheet()
            title_style = styles["Title"]
            
            if report_type == "work_hours":
                title = "工作时间报告"
                elements.append(Paragraph(title, title_style))
                elements.append(Spacer(1, 20))
                
                # 获取所有工人的工作时间数据
                with conn.cursor() as cur:
                    cur.execute(
                        """SELECT d.user_id, d.first_name, d.total_hours
                           FROM drivers d
                           ORDER BY d.first_name"""
                    )
                    workers = cur.fetchall()
                    
                    # 为每个工人获取本月的工作时间
                    data = [["工人姓名", "总工作时间", "本月工作时间", "本月工作天数"]]
                    
                    for worker in workers:
                        user_id, name, total_hours = worker
                        
                        # 获取本月工作天数
                        cur.execute(
                            """SELECT COUNT(DISTINCT date) 
                               FROM clock_logs 
                               WHERE user_id = %s 
                               AND date BETWEEN %s AND %s
                               AND is_off = FALSE""",
                            (user_id, first_day, last_day)
                        )
                        work_days = cur.fetchone()[0] or 0
                        
                        # 计算本月工作时间
                        month_hours = 0
                        cur.execute(
                            """SELECT date, clock_in, clock_out, is_off
                               FROM clock_logs 
                               WHERE user_id = %s 
                               AND date BETWEEN %s AND %s""",
                            (user_id, first_day, last_day)
                        )
                        logs = cur.fetchall()
                        
                        for log in logs:
                            _, clock_in, clock_out, is_off = log
                            if not is_off and clock_in and clock_out and clock_in != 'OFF' and clock_out != 'OFF':
                                try:
                                    if isinstance(clock_in, str) and isinstance(clock_out, str):
                                        in_time = datetime.datetime.strptime(clock_in, "%Y-%m-%d %H:%M:%S")
                                        out_time = datetime.datetime.strptime(clock_out, "%Y-%m-%d %H:%M:%S")
                                        hours = (out_time - in_time).total_seconds() / 3600
                                        if hours > 0:
                                            month_hours += hours
                                except (ValueError, TypeError):
                                    pass
                        
                        data.append([
                            name, 
                            f"{format_duration(total_hours)}", 
                            f"{format_duration(month_hours)}", 
                            f"{work_days}"
                        ])
                    
                    # 创建表格
                    table = Table(data)
                    table.setStyle(TableStyle([
                        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                        ('GRID', (0, 0), (-1, -1), 1, colors.black),
                    ]))
                    elements.append(table)
            
            elif report_type == "salary":
                title = "薪资报告"
                elements.append(Paragraph(title, title_style))
                elements.append(Spacer(1, 20))
                
                # 获取所有工人的薪资数据
                with conn.cursor() as cur:
                    cur.execute(
                        """SELECT d.user_id, d.first_name, d.monthly_salary, d.balance
                           FROM drivers d
                           ORDER BY d.first_name"""
                    )
                    workers = cur.fetchall()
                    
                    # 为每个工人获取本月的薪资信息
                    data = [["工人姓名", "月薪 (RM)", "当前余额 (RM)", "本月报销 (RM)"]]
                    
                    for worker in workers:
                        user_id, name, monthly_salary, balance = worker
                        
                        # 获取本月报销金额
                        cur.execute(
                            """SELECT COALESCE(SUM(amount), 0)
                               FROM claims 
                               WHERE user_id = %s 
                               AND date BETWEEN %s AND %s""",
                            (user_id, first_day, last_day)
                        )
                        claims_amount = cur.fetchone()[0] or 0
                        
                        data.append([
                            name, 
                            f"{monthly_salary:.2f}", 
                            f"{balance:.2f}", 
                            f"{claims_amount:.2f}"
                        ])
                    
                    # 创建表格
                    table = Table(data)
                    table.setStyle(TableStyle([
                        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                        ('GRID', (0, 0), (-1, -1), 1, colors.black),
                    ]))
                    elements.append(table)
            
            else:  # all
                title = "全部数据报告"
                elements.append(Paragraph(title, title_style))
                elements.append(Spacer(1, 20))
                
                # 工人基本信息
                elements.append(Paragraph("工人基本信息", styles["Heading2"]))
                elements.append(Spacer(1, 10))
                
                with conn.cursor() as cur:
                    cur.execute(
                        """SELECT d.user_id, d.first_name, d.monthly_salary, d.total_hours, d.balance
                           FROM drivers d
                           ORDER BY d.first_name"""
                    )
                    workers = cur.fetchall()
                    
                    data = [["工人姓名", "月薪 (RM)", "总工作时间", "当前余额 (RM)"]]
                    for worker in workers:
                        user_id, name, monthly_salary, total_hours, balance = worker
                        data.append([
                            name, 
                            f"{monthly_salary:.2f}", 
                            f"{format_duration(total_hours)}", 
                            f"{balance:.2f}"
                        ])
                    
                    table = Table(data)
                    table.setStyle(TableStyle([
                        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                        ('GRID', (0, 0), (-1, -1), 1, colors.black),
                    ]))
                    elements.append(table)
                    elements.append(Spacer(1, 20))
                    
                    # 本月打卡记录
                    elements.append(Paragraph("本月打卡记录", styles["Heading2"]))
                    elements.append(Spacer(1, 10))
                    
                    for worker in workers:
                        user_id, name, _, _, _ = worker
                        elements.append(Paragraph(f"工人: {name}", styles["Heading3"]))
                        elements.append(Spacer(1, 5))
                        
                        cur.execute(
                            """SELECT date, clock_in, clock_out, is_off
                               FROM clock_logs 
                               WHERE user_id = %s 
                               AND date BETWEEN %s AND %s
                               ORDER BY date DESC""",
                            (user_id, first_day, last_day)
                        )
                        logs = cur.fetchall()
                        
                        if logs:
                            log_data = [["日期", "上班时间", "下班时间", "休息日", "工作时长"]]
                            
                            for log in logs:
                                date, clock_in, clock_out, is_off = log
                                
                                # 计算工作时长
                                hours = 0
                                if not is_off and clock_in and clock_out and clock_in != 'OFF' and clock_out != 'OFF':
                                    try:
                                        if isinstance(clock_in, str) and isinstance(clock_out, str):
                                            in_time = datetime.datetime.strptime(clock_in, "%Y-%m-%d %H:%M:%S")
                                            out_time = datetime.datetime.strptime(clock_out, "%Y-%m-%d %H:%M:%S")
                                            hours = (out_time - in_time).total_seconds() / 3600
                                    except (ValueError, TypeError):
                                        pass
                                
                                log_data.append([
                                    date.strftime("%Y-%m-%d"),
                                    "休息日" if is_off else (clock_in if clock_in else "未打卡"),
                                    "休息日" if is_off else (clock_out if clock_out else "未打卡"),
                                    "是" if is_off else "否",
                                    format_duration(hours) if hours > 0 else "-"
                                ])
                            
                            log_table = Table(log_data)
                            log_table.setStyle(TableStyle([
                                ('BACKGROUND', (0, 0), (-1, 0), colors.lightgrey),
                                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                                ('GRID', (0, 0), (-1, -1), 1, colors.black),
                            ]))
                            elements.append(log_table)
                        else:
                            elements.append(Paragraph("没有打卡记录", styles["Normal"]))
                        
                        elements.append(Spacer(1, 15))
            
            # 构建PDF
            doc.build(elements)
            
            # 发送PDF文件
            with open(pdf_path, 'rb') as f:
                current_date = datetime.datetime.now().strftime("%Y%m%d")
                bot.send_document(
                    chat_id=user.id,
                    document=f,
                    filename=f"{report_type}_report_{current_date}.pdf",
                    caption=f"📊 {title} - 生成于 {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}"
                )
            
            # 删除临时文件
            os.unlink(pdf_path)
            
            # 更新消息
            query.edit_message_text(f"✅ {title}已生成并发送！")
            
        finally:
            release_db_connection(conn)
            
    except Exception as e:
        logger.error(f"Error generating PDF: {str(e)}")
        query.edit_message_text("❌ 生成报告时出错。请稍后再试或联系管理员。")

def viewclaims_start(update, context):
    """开始查看报销记录流程"""
    user = update.effective_user
    if user.id not in ADMIN_IDS:
        update.message.reply_text("❌ This command is only available for admins.")
        return ConversationHandler.END
    
    return show_workers_page(update, context, page=1, command="viewclaims")

def show_workers_page(update, context, page=1, command=""):
    """显示工人列表的分页"""
    items_per_page = 5
    offset = (page - 1) * items_per_page
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # 获取总数
            cur.execute("SELECT COUNT(*) FROM drivers")
            total_workers = cur.fetchone()[0]
            
            # 获取当前页的工人
            cur.execute(
                """SELECT user_id, first_name 
                   FROM drivers 
                   ORDER BY first_name 
                   LIMIT %s OFFSET %s""",
                (items_per_page, offset)
            )
            workers = cur.fetchall()
            
            if not workers:
                update.message.reply_text("No workers found.")
                return ConversationHandler.END
            
            # 创建键盘按钮
            keyboard = []
            for worker in workers:
                user_id, name = worker
                keyboard.append([f"{user_id} - {name}"])
            
            # 添加导航按钮
            nav_buttons = []
            if page > 1:
                nav_buttons.append(f"◀️ Previous")
            if (page * items_per_page) < total_workers:
                nav_buttons.append(f"Next ▶️")
            if nav_buttons:
                keyboard.append(nav_buttons)
            
            # 添加取消按钮
            keyboard.append(["❌ Cancel"])
            
            reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True)
            
            # 保存当前页码和命令到上下文
            context.user_data['current_page'] = page
            context.user_data['current_command'] = command
            
            update.message.reply_text(
                f"Select a worker (Page {page}):",
                reply_markup=reply_markup
            )
            
            if command == "viewclaims":
                return VIEWCLAIMS_SELECT_USER
            elif command == "checkstate":
                return CHECKSTATE_SELECT_USER
            elif command == "paid":
                return PAID_SELECT_DRIVER
            return ConversationHandler.END
            
    except Exception as e:
        logger.error(f"Error in show_workers_page: {str(e)}")
        update.message.reply_text("❌ An error occurred. Please try again.")
        return ConversationHandler.END
    finally:
        release_db_connection(conn)

def handle_page_navigation(update, context):
    """处理分页导航"""
    text = update.message.text
    current_page = context.user_data.get('current_page', 1)
    command = context.user_data.get('current_command', '')
    
    if text == "◀️ Previous":
        return show_workers_page(update, context, page=current_page-1, command=command)
    elif text == "Next ▶️":
        return show_workers_page(update, context, page=current_page+1, command=command)
    return None

def viewclaims_select_user(update, context):
    """选择要查看报销记录的用户"""
    # 检查是否是导航命令
    nav_result = handle_page_navigation(update, context)
    if nav_result is not None:
        return nav_result
        
    try:
        user_id = int(update.message.text.split()[0])
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT type, amount, date, status 
                       FROM claims 
                       WHERE user_id = %s 
                       ORDER BY date DESC 
                       LIMIT 5""",
                    (user_id,)
                )
                claims = cur.fetchall()
                
                if not claims:
                    update.message.reply_text(
                        "📝 No claims found.",
                        reply_markup=ReplyKeyboardRemove()
                    )
                    return ConversationHandler.END
                
                message = ["📋 Recent Claims:"]
                for claim in claims:
                    claim_type, amount, date, status = claim
                    message.append(
                        f"\n{date.strftime('%Y-%m-%d')}"
                        f"\nType: {claim_type}"
                        f"\nAmount: RM {amount:.2f}"
                        f"\nStatus: {status}"
                        f"\n{'-'*20}"
                    )
                
                update.message.reply_text(
                    "".join(message),
                    reply_markup=ReplyKeyboardRemove()
                )
        finally:
            release_db_connection(conn)
        
        return ConversationHandler.END
    except (ValueError, IndexError):
        update.message.reply_text(
            "❌ Please select a valid worker.",
            reply_markup=ReplyKeyboardRemove()
        )
        return ConversationHandler.END

def viewclaims(update, context):
    """查看报销记录"""
    user = update.effective_user
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT type, amount, date, status 
                   FROM claims 
                   WHERE user_id = %s 
                   ORDER BY date DESC 
                   LIMIT 5""",
                (user.id,)
            )
            claims = cur.fetchall()
            
            if not claims:
                update.message.reply_text("📝 No claims found.")
                return
            
            message = ["📋 Recent Claims:"]
            for claim in claims:
                claim_type, amount, date, status = claim
                message.append(
                    f"\n{date.strftime('%Y-%m-%d')}"
                    f"\nType: {claim_type}"
                    f"\nAmount: RM {amount:.2f}"
                    f"\nStatus: {status}"
                    f"\n{'-'*20}"
                )
            
            update.message.reply_text("".join(message))
    except Exception as e:
        logger.error(f"Error in viewclaims: {str(e)}")
        update.message.reply_text("❌ An error occurred. Please try again.")
    finally:
        release_db_connection(conn)

def balance(update, context):
    """查看余额"""
    user = update.effective_user
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT balance, monthly_salary, total_hours FROM drivers WHERE user_id = %s",
                (user.id,)
            )
            result = cur.fetchone()
            
            if not result:
                update.message.reply_text("❌ User not found.")
                return
            
            balance, monthly_salary, total_hours = result
            
            # 获取本月的报销总额
            cur.execute(
                """SELECT COALESCE(SUM(amount), 0) FROM claims 
                   WHERE user_id = %s AND 
                   date_trunc('month', date) = date_trunc('month', CURRENT_DATE)""",
                (user.id,)
            )
            claims_total = cur.fetchone()[0]
            
            update.message.reply_text(
                f"💰 Balance Summary\n\n"
                f"Current Balance: RM {balance:.2f}\n"
                f"Monthly Salary: RM {monthly_salary:.2f}\n"
                f"Total Hours: {format_duration(total_hours)}\n"
                f"This Month Claims: RM {claims_total:.2f}"
            )
    except Exception as e:
        logger.error(f"Error in balance: {str(e)}")
        update.message.reply_text("❌ An error occurred. Please try again.")
    finally:
        release_db_connection(conn)

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

def checkstate_start(update, context):
    """开始查看状态流程"""
    user = update.effective_user
    if user.id not in ADMIN_IDS:
        update.message.reply_text("❌ This command is only available for admins.")
        return ConversationHandler.END
    
    return show_workers_page(update, context, page=1, command="checkstate")

def checkstate_select_user(update, context):
    """选择要查看状态的用户"""
    # 检查是否是导航命令
    nav_result = handle_page_navigation(update, context)
    if nav_result is not None:
        return nav_result
        
    try:
        # 记录日志，帮助调试
        logger.info(f"checkstate_select_user received text: '{update.message.text}'")
        
        user_id = int(update.message.text.split()[0])
        logger.info(f"Attempting to get status for user_id: {user_id}")
        
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                # 获取基本信息
                cur.execute(
                    """SELECT first_name, monthly_salary, total_hours 
                       FROM drivers 
                       WHERE user_id = %s""",
                    (user_id,)
                )
                basic_info = cur.fetchone()
                
                if not basic_info:
                    update.message.reply_text(
                        "❌ Worker not found.",
                        reply_markup=ReplyKeyboardRemove()
                    )
                    return ConversationHandler.END
                
                name, monthly_salary, total_hours = basic_info
                
                # 获取本月工作天数和休息日
                cur.execute(
                    """SELECT 
                        COUNT(DISTINCT date) as work_days,
                        COUNT(DISTINCT CASE WHEN is_off THEN date END) as off_days
                    FROM clock_logs 
                    WHERE user_id = %s 
                    AND date_trunc('month', date) = date_trunc('month', CURRENT_DATE)""",
                    (user_id,)
                )
                work_stats = cur.fetchone() or (0, 0)
                work_days, off_days = work_stats
                
                # 获取本月工作时长 - 使用更安全的方法计算时间差
                cur.execute(
                    """SELECT 
                        date, 
                        clock_in, 
                        clock_out, 
                        is_off
                    FROM clock_logs 
                    WHERE user_id = %s 
                    AND date_trunc('month', date) = date_trunc('month', CURRENT_DATE)""",
                    (user_id,)
                )
                logs = cur.fetchall()
                
                # 手动计算工作时长，避免SQL中的时间戳转换问题
                month_hours = 0
                for log in logs:
                    date, clock_in, clock_out, is_off = log
                    if not is_off and clock_in and clock_out and clock_in != 'OFF' and clock_out != 'OFF':
                        try:
                            # 尝试解析时间戳
                            if isinstance(clock_in, str) and isinstance(clock_out, str):
                                in_time = datetime.datetime.strptime(clock_in, "%Y-%m-%d %H:%M:%S")
                                out_time = datetime.datetime.strptime(clock_out, "%Y-%m-%d %H:%M:%S")
                                hours = (out_time - in_time).total_seconds() / 3600
                                if hours > 0:
                                    month_hours += hours
                        except (ValueError, TypeError) as e:
                            logger.warning(f"Error parsing timestamps for date {date}: {e}")
                
                # 获取本月 OT 时长
                cur.execute(
                    """SELECT COALESCE(SUM(duration), 0) as total_ot_hours
                       FROM ot_logs 
                       WHERE user_id = %s 
                       AND date_trunc('month', date) = date_trunc('month', CURRENT_DATE)
                       AND end_time IS NOT NULL""",
                    (user_id,)
                )
                ot_hours = cur.fetchone()[0] or 0
                ot_hours_int = int(ot_hours)
                ot_minutes = int((ot_hours - ot_hours_int) * 60)
                
                # 获取报销总额
                cur.execute(
                    """SELECT COALESCE(SUM(amount), 0) as total_claims
                       FROM claims 
                       WHERE user_id = %s""",
                    (user_id,)
                )
                total_claims = cur.fetchone()[0] or 0
                
                message = [
                    f"📊 Worker Status: {name}\n",
                    f"💰 Monthly Salary: RM {monthly_salary:.2f}",
                    f"⏰ Total Work Hours (All time): {format_duration(total_hours)}",
                    f"⏰ This Month Hours: {format_duration(month_hours)}",
                    f"🕒 This Month OT: {ot_hours_int}h {ot_minutes}m",
                    f"📅 This Month Work Days: {work_days} days",
                    f"🏖 This Month Off Days: {off_days} days",
                    f"💵 Total Claims: RM {total_claims:.2f}"
                ]
                
                update.message.reply_text(
                    "\n".join(message),
                    reply_markup=ReplyKeyboardRemove()
                )
                logger.info(f"Successfully sent status for user {user_id}")
                
        except Exception as e:
            logger.error(f"Database error in checkstate_select_user: {str(e)}")
            update.message.reply_text(
                "❌ Database error occurred. Please try again or contact admin.",
                reply_markup=ReplyKeyboardRemove()
            )
        finally:
            release_db_connection(conn)
        
        return ConversationHandler.END
    except (ValueError, IndexError) as e:
        logger.error(f"Error parsing user input in checkstate_select_user: {str(e)}")
        update.message.reply_text(
            "❌ Please select a valid worker.",
            reply_markup=ReplyKeyboardRemove()
        )
        return ConversationHandler.END
    except Exception as e:
        logger.error(f"Unexpected error in checkstate_select_user: {str(e)}")
        update.message.reply_text(
            "❌ An error occurred. Please try again or contact admin.",
            reply_markup=ReplyKeyboardRemove()
        )
        return ConversationHandler.END

def ot(update, context):
    """处理 OT 命令"""
    user = update.effective_user
    now = get_current_time()
    today = now.date()
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # 检查是否有未完成的 OT 记录
            cur.execute(
                """SELECT id, start_time 
                   FROM ot_logs 
                   WHERE user_id = %s AND date = %s AND end_time IS NULL""",
                (user.id, today)
            )
            ongoing_ot = cur.fetchone()
            
            if not ongoing_ot:
                # 开始新的 OT
                cur.execute(
                    """INSERT INTO ot_logs (user_id, date, start_time)
                       VALUES (%s, %s, %s)""",
                    (user.id, today, now)
                )
                conn.commit()
                
                update.message.reply_text(
                    "🕒 OT Started!\n"
                    "Use /OT command again to end your OT session."
                )
            else:
                # 结束现有的 OT
                ot_id, start_time = ongoing_ot
                duration = (now - start_time).total_seconds() / 3600  # 转换为小时
                
                cur.execute(
                    """UPDATE ot_logs 
                       SET end_time = %s, duration = %s 
                       WHERE id = %s""",
                    (now, duration, ot_id)
                )
                conn.commit()
                
                hours = int(duration)
                minutes = int((duration - hours) * 60)
                
                update.message.reply_text(
                    f"✅ OT Completed!\n"
                    f"Duration: {hours}h {minutes}m\n"
                    f"Start: {format_local_time(start_time)}\n"
                    f"End: {format_local_time(now)}"
                )
    except Exception as e:
        logger.error(f"Error in OT command: {str(e)}")
        update.message.reply_text("❌ An error occurred. Please try again.")
    finally:
        release_db_connection(conn) 
