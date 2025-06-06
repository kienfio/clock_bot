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

# === 全局变量 ===
# 从环境变量加载配置
env_path = Path('.') / '.env'
load_dotenv(dotenv_path=env_path)

TOKEN = os.getenv("TOKEN")
ADMIN_IDS = list(map(int, os.getenv("ADMIN_IDS", "1165249082").split(",")))
GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')

# === Telegram Bot 设置 ===
bot = Bot(token=TOKEN)

# === 日志设置 ===
logger = logging.getLogger(__name__)

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
                
                conn.commit()
                logger.info("Database tables created successfully")
        finally:
            release_db_connection(conn)
            
    except Exception as e:
        logger.error(f"Database initialization failed: {str(e)}")
        raise

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

# === Command Handlers ===
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
                # 创建新用户，确保工资为0
                cur.execute(
                    """INSERT INTO drivers (user_id, username, first_name, monthly_salary) 
                       VALUES (%s, %s, %s, 0.0)""",
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
                    "💸 /claim\n"
                    "⏰ /OT\n\n"
                    "🔐 Admin Commands:\n"
                    "📊 /checkstate\n"
                    "🧾 /PDF\n"
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
                    "💸 /claim\n"
                    "⏰ /OT\n\n"
                    "🔐 Admin Commands:\n"
                    "📊 /checkstate\n"
                    "🧾 /PDF\n"
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

def clockout(update, context):
    """处理下班打卡"""
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
