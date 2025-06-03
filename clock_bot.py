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
GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')  # 添加 Google API Key

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
PAID_START_DATE = 1  # 新增开始日期状态
PAID_END_DATE = 2    # 新增结束日期状态

# === 数据库连接池 ===
db_pool = None

def init_db():
    """初始化数据库和表结构"""
    global db_pool
    
    # 从环境变量获取数据库连接信息
    DATABASE_URL = os.environ.get("DATABASE_URL")
    
    if not DATABASE_URL:
        raise ValueError("需要设置 DATABASE_URL 环境变量")
    
    logger.info("开始初始化数据库...")
    
    try:
        # 创建连接池
        db_pool = psycopg2.pool.SimpleConnectionPool(
            minconn=1,
            maxconn=20,
            dsn=DATABASE_URL
        )
        logger.info("数据库连接池创建成功")
        
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                # 1. 创建 drivers 表（添加 timezone 列）
                cur.execute("""
                CREATE TABLE IF NOT EXISTS drivers (
                    user_id BIGINT PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    balance FLOAT DEFAULT 0.0,
                    monthly_salary FLOAT DEFAULT 3500.0,
                    total_hours FLOAT DEFAULT 0.0,
                    timezone TEXT DEFAULT 'UTC',
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                )
                """)
                logger.info("创建 drivers 表成功")
                
                # 确保 timezone 列存在（兼容旧表结构）
                cur.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 
                        FROM information_schema.columns 
                        WHERE table_name='drivers' AND column_name='timezone'
                    ) THEN
                        ALTER TABLE drivers ADD COLUMN timezone TEXT DEFAULT 'UTC';
                    END IF;
                END $$;
                """)
                logger.info("确保 drivers 表存在 timezone 列")
                
                # 2. 打卡记录表
                cur.execute("""
                CREATE TABLE IF NOT EXISTS clock_logs (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT REFERENCES drivers(user_id),
                    date DATE NOT NULL,
                    clock_in TIMESTAMP WITH TIME ZONE,
                    clock_out TIMESTAMP WITH TIME ZONE,
                    is_off BOOLEAN DEFAULT FALSE,
                    location_address TEXT,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(user_id, date)
                )
                """)
                logger.info("创建 clock_logs 表成功")
                
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
                logger.info("确保 clock_logs 表存在 location_address 列")
                
                # 3. 充值记录表
                cur.execute("""
                CREATE TABLE IF NOT EXISTS topups (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT REFERENCES drivers(user_id),
                    amount FLOAT NOT NULL,
                    date DATE NOT NULL,
                    admin_id BIGINT,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                )
                """)
                logger.info("创建 topups 表成功")
                
                # 4. 报销记录表
                cur.execute("""
                CREATE TABLE IF NOT EXISTS claims (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT REFERENCES drivers(user_id),
                    type TEXT NOT NULL,
                    amount FLOAT NOT NULL,
                    date DATE NOT NULL,
                    photo_file_id TEXT,
                    status TEXT DEFAULT 'pending',
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                )
                """)
                logger.info("创建 claims 表成功")
                
                # 5. 创建索引
                cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_clock_logs_user_date ON clock_logs(user_id, date);
                CREATE INDEX IF NOT EXISTS idx_claims_user_date ON claims(user_id, date);
                CREATE INDEX IF NOT EXISTS idx_topups_user_date ON topups(user_id, date);
                CREATE INDEX IF NOT EXISTS idx_drivers_timezone ON drivers(timezone);
                """)
                logger.info("创建索引成功")
                
                conn.commit()
                logger.info("数据库初始化完成！")
        finally:
            release_db_connection(conn)
            
    except Exception as e:
        logger.error(f"数据库初始化失败: {str(e)}")
        raise

def get_db_connection():
    """获取数据库连接"""
    try:
        conn = db_pool.getconn()
        # 设置连接的时区为 UTC
        with conn.cursor() as cur:
            cur.execute("SET TIME ZONE 'UTC';")
        return conn
    except psycopg2.pool.PoolError:
        logger.error("连接池已满，等待可用连接...")
        # 等待一会儿再试
        time.sleep(1)
        try:
            conn = db_pool.getconn()
            # 设置连接的时区为 UTC
            with conn.cursor() as cur:
                cur.execute("SET TIME ZONE 'UTC';")
            return conn
        except Exception as e:
            logger.error(f"获取数据库连接失败: {e}")
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

def get_driver(user_id):
    """获取司机信息"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM drivers WHERE user_id = %s", (user_id,))
            return cur.fetchone()
    finally:
        release_db_connection(conn)

def update_driver(user_id, username=None, first_name=None, balance=None, monthly_salary=None, total_hours=None):
    """更新司机信息"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # 检查司机是否存在
            cur.execute("SELECT 1 FROM drivers WHERE user_id = %s", (user_id,))
            if not cur.fetchone():
                # 插入新司机
                cur.execute(
                    "INSERT INTO drivers (user_id, username, first_name) VALUES (%s, %s, %s)",
                    (user_id, username, first_name)
                )
            
            updates = []
            params = []
            
            if username is not None:
                updates.append("username = %s")
                params.append(username)
            if first_name is not None:
                updates.append("first_name = %s")
                params.append(first_name)
            if balance is not None:
                updates.append("balance = %s")
                params.append(balance)
            if monthly_salary is not None:
                updates.append("monthly_salary = %s")
                params.append(monthly_salary)
            if total_hours is not None:
                updates.append("total_hours = %s")
                params.append(total_hours)
            
            if updates:
                query = "UPDATE drivers SET " + ", ".join(updates) + " WHERE user_id = %s"
                params.append(user_id)
                cur.execute(query, params)
            
            conn.commit()
    finally:
        release_db_connection(conn)

def format_local_time(timestamp):
    """格式化时间为本地时间字符串"""
    if isinstance(timestamp, datetime.datetime):
        return timestamp.strftime("%Y-%m-%d %H:%M")
    try:
        dt = datetime.datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
        return dt.strftime("%Y-%m-%d %H:%M")
    except:
        return str(timestamp)

def format_duration(hours):
    try:
        total_minutes = int(float(hours) * 60)
        hours_part = total_minutes // 60
        minutes_part = total_minutes % 60
        
        if hours_part > 0 and minutes_part > 0:
            return f"{hours_part}Hour {minutes_part}Min"
        elif hours_part > 0:
            return f"{hours_part}Hour"
        else:
            return f"{minutes_part}Min"
    except:
        return str(hours)

def get_month_date_range(date=None):
    if date is None:
        date = datetime.datetime.now(pytz.timezone("Asia/Kuala_Lumpur"))
    
    year = date.year
    month = date.month
    first_day = datetime.date(year, month, 1)
    last_day = datetime.date(year, month, calendar.monthrange(year, month)[1])
    return first_day, last_day

def calculate_hourly_rate(monthly_salary):
    try:
        return round(float(monthly_salary) / (WORKING_DAYS_PER_MONTH * WORKING_HOURS_PER_DAY), 2)
    except:
        return DEFAULT_HOURLY_RATE

# === PDF 生成功能 ===
def download_telegram_photo(file_id, bot):
    try:
        file = bot.get_file(file_id)
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.jpg')
        file.download(temp_file.name)
        return temp_file.name
    except Exception as e:
        logger.error(f"Error downloading photo: {str(e)}")
        return None

def generate_driver_pdf(driver_id, driver_name, bot, output_path):
    """生成司机PDF报告"""
    doc = SimpleDocTemplate(
        output_path,
        pagesize=A4,
        rightMargin=72,
        leftMargin=72,
        topMargin=72,
        bottomMargin=72
    )
    
    styles = getSampleStyleSheet()
    elements = []
    
    # 获取司机数据
    with db_pool.getconn() as conn:
        with conn.cursor() as cur:
            # 基本信息
            cur.execute("SELECT * FROM drivers WHERE user_id = %s", (driver_id,))
            driver = cur.fetchone()
            
            # 打卡记录
            cur.execute("""
            SELECT date, clock_in, clock_out, is_off 
            FROM clock_logs 
            WHERE user_id = %s 
            ORDER BY date DESC
            """, (driver_id,))
            clock_logs = cur.fetchall()
            
            # 报销记录
            cur.execute("""
            SELECT type, amount, date, photo_file_id 
            FROM claims 
            WHERE user_id = %s 
            ORDER BY date DESC
            """, (driver_id,))
            claims = cur.fetchall()
            
            # 充值记录
            cur.execute("""
            SELECT amount, date 
            FROM topups 
            WHERE user_id = %s 
            ORDER BY date DESC
            """, (driver_id,))
            topups = cur.fetchall()
    
    # 标题
    title = Paragraph(f"Driver Report: {driver_name}", styles['Title'])
    elements.append(title)
    elements.append(Spacer(1, 12))
    
    # 打卡记录表格
    elements.append(Paragraph("Daily Clock Records", styles['Heading2']))
    clock_data = [['Date', 'Clock In', 'Clock Out', 'Hours']]
    total_hours = driver[5] if driver else 0.0
    
    for log in clock_logs:
        date, in_time, out_time, is_off = log
        date_str = date.strftime("%Y-%m-%d")
        
        if is_off:
            clock_data.append([date_str, "OFF", "OFF", "OFF"])
            continue
            
        in_time_str = format_local_time(in_time) if in_time else "N/A"
        out_time_str = format_local_time(out_time) if out_time else "N/A"
        
        hours = "N/A"
        if in_time and out_time:
            try:
                in_dt = datetime.datetime.strptime(in_time, "%Y-%m-%d %H:%M:%S")
                out_dt = datetime.datetime.strptime(out_time, "%Y-%m-%d %H:%M:%S")
                duration = out_dt - in_dt
                hours_float = duration.total_seconds() / 3600
                hours = format_duration(hours_float)
            except:
                hours = "Error"
                
        clock_data.append([date_str, in_time_str, out_time_str, hours])
    
    if len(clock_data) > 1:
        clock_table = Table(clock_data, colWidths=[80, 120, 120, 60])
        clock_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('ALIGN', (0, 1), (-1, -1), 'CENTER'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ]))
        elements.append(clock_table)
    else:
        elements.append(Paragraph("No clock records found.", styles['Normal']))
    
    elements.append(Spacer(1, 20))
    
    # 报销记录
    elements.append(Paragraph("Expense Claims", styles['Heading2']))
    
    if claims:
        for claim in claims:
            claim_type, amount, date, photo_id = claim
            claim_data = [
                [f"Date: {date}", f"Type: {claim_type}", f"Amount: RM{amount:.2f}"]
            ]
            
            claim_table = Table(claim_data, colWidths=[120, 120, 120])
            claim_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.lightblue),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ]))
            elements.append(claim_table)
            
            if photo_id:
                try:
                    photo_path = download_telegram_photo(photo_id, bot)
                    if photo_path:
                        img = Image(photo_path, width=300, height=200)
                        elements.append(img)
                        elements.append(Spacer(1, 6))
                except Exception as e:
                    elements.append(Paragraph(f"Error loading photo: {str(e)}", styles['Normal']))
            
            elements.append(Spacer(1, 10))
    else:
        elements.append(Paragraph("No claims found.", styles['Normal']))
    
    elements.append(Spacer(1, 20))
    
    # 摘要部分
    elements.append(Paragraph("Summary", styles['Heading2']))
    
    first_day, last_day = get_month_date_range()
    elements.append(Paragraph(
        f"Summary Period: {first_day.strftime('%Y-%m-%d')} to {last_day.strftime('%Y-%m-%d')}",
        styles['Normal']
    ))
    
    hourly_rate = calculate_hourly_rate(driver[4]) if driver else DEFAULT_HOURLY_RATE
    monthly_salary = f"RM{driver[4]:.2f}" if driver else "N/A"
    gross_pay = total_hours * hourly_rate
    
    elements.append(Paragraph(
        f"Monthly Salary: {monthly_salary}\n"
        f"Hourly Rate: RM{hourly_rate:.2f}\n"
        f"Total Hours: {format_duration(total_hours)}\n"
        f"Gross Pay: RM{gross_pay:.2f}",
        styles['Normal']
    ))
    
    # 账户摘要
    total_claims = sum(claim[1] for claim in claims)
    balance = driver[3] if driver else 0.0
    
    summary_data = [
        ['Total Hours', 'Total Claims', 'Account Balance'],
        [format_duration(total_hours), f"RM{total_claims:.2f}", f"RM{balance:.2f}"]
    ]
    
    summary_table = Table(summary_data, colWidths=[120, 120, 120])
    summary_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.darkblue),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.lightgrey),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
    ]))
    elements.append(summary_table)
    
    doc.build(elements)
    return output_path

# === 添加位置识别功能 ===
def get_timezone_from_location(latitude, longitude):
    """根据经纬度获取时区"""
    try:
        timestamp = int(time.time())
        url = f"https://maps.googleapis.com/maps/api/timezone/json?location={latitude},{longitude}&timestamp={timestamp}&key={GOOGLE_API_KEY}"
        response = requests.get(url)
        data = response.json()
        
        if data['status'] == 'OK':
            return data['timeZoneId']
        else:
            logger.error(f"Error getting timezone: {data}")
            return DEFAULT_TIMEZONE
    except Exception as e:
        logger.error(f"Error in get_timezone_from_location: {e}")
        return DEFAULT_TIMEZONE

def update_user_timezone(user_id, latitude, longitude):
    """更新用户时区"""
    timezone = get_timezone_from_location(latitude, longitude)
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE drivers SET timezone = %s WHERE user_id = %s",
                (timezone, user_id)
            )
            conn.commit()
        return timezone
    except Exception as e:
        logger.error(f"Error updating user timezone: {e}")
        return DEFAULT_TIMEZONE
    finally:
        release_db_connection(conn)

def handle_location(update, context):
    """处理用户发送的位置信息"""
    try:
        user = update.effective_user
        location = update.message.location
        
        # 获取地址
        address = get_address_from_location(location.latitude, location.longitude)
        
        # 获取打卡时间
        clockin_time = context.user_data.get('clockin_time', '')
        
        # 更新打卡记录中的地址
        today = get_current_date_for_user(user.id)
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE clock_logs SET location_address = %s WHERE user_id = %s AND date = %s",
                    (address, user.id, today)
                )
                conn.commit()
        finally:
            release_db_connection(conn)
        
        # 显示带地址的打卡确认
        update.message.reply_text(
            f"✅ Clocked in at {clockin_time}\n"
            f"⟶ \"{address}\"",
            reply_markup=ReplyKeyboardRemove()
        )
        
        # 清理用户数据
        if 'clockin_time' in context.user_data:
            del context.user_data['clockin_time']
            
    except Exception as e:
        logger.error(f"Error in handle_location: {e}")
        update.message.reply_text(
            "❌ Failed to process your location. Please try again later.",
            reply_markup=ReplyKeyboardRemove()
        )

def handle_text_after_clockin(update, context):
    """处理打卡后的文本消息（可能拒绝位置）"""
    try:
        user = update.effective_user
        message = update.message.text
        
        # 检查是否是打卡后的消息且是跳过位置的选项
        if 'clockin_time' in context.user_data and message == "Skip Location ⏭":
            clockin_time = context.user_data['clockin_time']
            
            # 更新打卡记录为拒绝位置
            today = get_current_date_for_user(user.id)
            conn = get_db_connection()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE clock_logs SET location_address = 'refuse report location' "
                        "WHERE user_id = %s AND date = %s",
                        (user.id, today)
                    )
                    conn.commit()
            finally:
                release_db_connection(conn)
            
            # 显示拒绝位置的消息
            update.message.reply_text(
                f"✅ Clocked in at {clockin_time}\n"
                "⟶ \"refuse report location\"",
                reply_markup=ReplyKeyboardRemove()
            )
            
            # 清理用户数据
            del context.user_data['clockin_time']
    except Exception as e:
        logger.error(f"Error in handle_text_after_clockin: {e}")

# === 命令处理函数 ===
def start(update, context):
    user = update.effective_user
    update_driver(
        user.id,
        username=user.username,
        first_name=user.first_name
    )
    
    msg = (
        f"👋 Hello {user.first_name}!\n"
        "Welcome to Driver ClockIn Bot.\n\n"
        "Available Commands:\n"
        "🕑 /clockin\n"
        "🏁 /clockout\n"
        "📅 /offday\n"
        "💸 /claim"
    )
    if user.id in ADMIN_IDS:
        msg += (
            "\n\n🔐 Admin Commands:\n"
            "📊 /balance\n"
            "📄 /check\n"
            "🧾 /PDF\n"
            "💵 /topup\n"
            "📷 /viewclaims\n"
            "💰 /salary\n"
            "🟢 /paid"  # 添加 paid 命令
        )

    update.message.reply_text(msg)

def clockin(update, context):
    try:
        user = update.effective_user
        now = get_current_time_for_user(user.id)
        today = now.date()
        clock_time = now.astimezone(pytz.UTC)
        
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                # 检查是否已有记录
                cur.execute(
                    "SELECT 1 FROM clock_logs WHERE user_id = %s AND date = %s",
                    (user.id, today)
                )
                if cur.fetchone():
                    # 更新记录
                    cur.execute(
                        "UPDATE clock_logs SET clock_in = %s, is_off = FALSE WHERE user_id = %s AND date = %s",
                        (clock_time, user.id, today)
                    )
                else:
                    # 插入新记录
                    cur.execute(
                        "INSERT INTO clock_logs (user_id, date, clock_in) VALUES (%s, %s, %s)",
                        (user.id, today, clock_time)
                    )
                conn.commit()
        finally:
            release_db_connection(conn)
        
        # 显示用户时区的时间
        local_time = clock_time.astimezone(pytz.timezone(get_user_timezone(user.id)))
        time_str = format_local_time(local_time)
        
        # 请求位置
        keyboard = [
            [KeyboardButton("Share Location 📍", request_location=True)],
            [KeyboardButton("Skip Location ⏭")]
        ]
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True)
        
        # 存储打卡时间用于后续地址处理
        context.user_data['clockin_time'] = time_str
        
        update.message.reply_text(
            f"✅ Clocked in at {time_str}\n"
            "Please share your location or skip:",
            reply_markup=reply_markup
        )
    except Exception as e:
        logger.error(f"Error in clockin: {str(e)}")
        update.message.reply_text(
            "❌ An error occurred while clocking in. Please try again.",
            reply_markup=ReplyKeyboardRemove()
        )

def clockout(update, context):
    try:
        user = update.effective_user
        now = get_current_time_for_user(user.id)
        today = now.date()
        clock_time = now.astimezone(pytz.UTC)  # 转换为 UTC 时间存储
        
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                # 检查是否已打卡
                cur.execute(
                    "SELECT clock_in FROM clock_logs WHERE user_id = %s AND date = %s",
                    (user.id, today)
                )
                log = cur.fetchone()
                
                if not log or not log[0]:
                    update.message.reply_text("❌ You haven't clocked in today.")
                    return
                
                # 更新打卡时间 
                cur.execute(
                    "UPDATE clock_logs SET clock_out = %s WHERE user_id = %s AND date = %s",
                    (clock_time, user.id, today)
                )
                
                # 计算工时（使用 UTC 时间计算）
                in_time = log[0]  # 数据库中存储的是 UTC 时间
                hours_worked = (clock_time - in_time).total_seconds() / 3600
                
                # 更新总工时
                cur.execute(
                    "UPDATE drivers SET total_hours = total_hours + %s WHERE user_id = %s",
                    (hours_worked, user.id)
                )
                conn.commit()
                
                # 显示用户时区的时间
                local_time = clock_time.astimezone(pytz.timezone(get_user_timezone(user.id)))
                time_str = format_duration(hours_worked)
                update.message.reply_text(
                    f"🏁 Clocked out at {format_local_time(local_time)}. Worked {time_str}."
                )
        finally:
            release_db_connection(conn)
    except Exception as e:
        logger.error(f"Error in clockout: {str(e)}")
        update.message.reply_text("❌ An error occurred while clocking out. Please try again.")

def offday(update, context):
    try:
        user = update.effective_user
        today = get_current_date_for_user(user.id)
        
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                # 标记休息日
                cur.execute(
                    "INSERT INTO clock_logs (user_id, date, is_off) VALUES (%s, %s, TRUE) "
                    "ON CONFLICT (user_id, date) DO UPDATE SET is_off = TRUE, clock_in = NULL, clock_out = NULL",
                    (user.id, today)
                )
                conn.commit()
                update.message.reply_text(f"📅 Marked {today} as off day.")
        finally:
            release_db_connection(conn)
    except Exception as e:
        logger.error(f"Error in offday: {str(e)}")
        update.message.reply_text("❌ An error occurred while marking off day. Please try again.")

def balance(update, context):
    if update.effective_user.id not in ADMIN_IDS:
        return
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT user_id, first_name, username, balance FROM drivers")
            drivers = cur.fetchall()
    finally:
        release_db_connection(conn)
    
    msg = "📊 Driver Balances:\n"
    for driver in drivers:
        name = f"@{driver[2]}" if driver[2] else driver[1]
        msg += f"• {name}: RM{driver[3]:.2f}\n"
    
    update.message.reply_text(msg)

def check(update, context):
    if update.effective_user.id not in ADMIN_IDS:
        return
    
    today = get_current_date_for_user(update.effective_user.id)
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
            SELECT d.user_id, d.first_name, d.username, l.clock_in, l.clock_out, l.is_off
            FROM drivers d
            LEFT JOIN clock_logs l ON d.user_id = l.user_id AND l.date = %s
            """, (today,))
            logs = cur.fetchall()
    finally:
        release_db_connection(conn)
    
    msg = "📄 Today's Status:\n"
    for log in logs:
        user_id, first_name, username, in_time, out_time, is_off = log
        name = f"@{username}" if username else first_name
        
        if is_off:
            msg += f"• {name}: OFF DAY\n"
        else:
            in_str = format_local_time(in_time) if in_time else "❌"
            out_str = format_local_time(out_time) if out_time else "❌"
            msg += f"• {name}: IN: {in_str}, OUT: {out_str}\n"
    
    update.message.reply_text(msg)

def viewclaims(update, context):
    if update.effective_user.id not in ADMIN_IDS:
        return
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
            SELECT d.user_id, d.first_name, d.username, c.type, c.amount, c.date
            FROM claims c
            JOIN drivers d ON c.user_id = d.user_id
            ORDER BY c.date DESC
            LIMIT 20
            """)
            claims = cur.fetchall()
    finally:
        release_db_connection(conn)
    
    msg = "📷 Recent Claims:\n"
    for claim in claims:
        user_id, first_name, username, claim_type, amount, date = claim
        name = f"@{username}" if username else first_name
        msg += f"• {name}: RM{amount:.2f} ({claim_type}) on {date}\n"
    
    update.message.reply_text(msg)

# === 薪资设置功能 ===
def salary_start(update, context):
    """开始设置薪资"""
    try:
        if update.effective_user.id not in ADMIN_IDS:
            update.message.reply_text("❌ You don't have permission to use this command.")
            return ConversationHandler.END
        
        # 清理之前的状态
        context.user_data.clear()
        
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT user_id, first_name, username FROM drivers")
                drivers = cur.fetchall()
        finally:
            release_db_connection(conn)
        
        if not drivers:
            update.message.reply_text("❌ No drivers found in the system.")
            return ConversationHandler.END
        
        keyboard = [[f"{driver[1]} (ID: {driver[0]})"] for driver in drivers]
        context.user_data['salary_drivers'] = {f"{driver[1]} (ID: {driver[0]})": driver[0] for driver in drivers}
        
        update.message.reply_text(
            "👤 Select driver to set salary:\n"
            "Or use /cancel to cancel this operation.",
            reply_markup=ReplyKeyboardMarkup(keyboard, one_time_keyboard=True)
        )
        return SALARY_SELECT_DRIVER
    except Exception as e:
        logger.error(f"Error in salary_start: {str(e)}")
        context.user_data.clear()  # 确保清理状态
        update.message.reply_text(
            "❌ An error occurred. Please try /salary command again.",
            reply_markup=ReplyKeyboardRemove()
        )
        return ConversationHandler.END

def salary_select_driver(update, context):
    try:
        if update.message.text.startswith('/'):  # 如果是命令，结束对话
            context.user_data.clear()
            update.message.reply_text(
                "❌ Operation cancelled due to new command.",
                reply_markup=ReplyKeyboardRemove()
            )
            return ConversationHandler.END
            
        selected = update.message.text
        drivers = context.user_data.get('salary_drivers', {})
        
        if selected not in drivers:
            context.user_data.clear()
            update.message.reply_text(
                "❌ Invalid selection.",
                reply_markup=ReplyKeyboardRemove()
            )
            return ConversationHandler.END
        
        context.user_data['selected_driver'] = drivers[selected]
        update.message.reply_text(
            "💰 Enter monthly salary (RM):\n"
            "Or use /cancel to cancel this operation.",
            reply_markup=ReplyKeyboardRemove()
        )
        return SALARY_ENTER_AMOUNT
    except Exception as e:
        logger.error(f"Error in salary_select_driver: {str(e)}")
        context.user_data.clear()  # 确保清理状态
        update.message.reply_text(
            "❌ An error occurred. Please try /salary command again.",
            reply_markup=ReplyKeyboardRemove()
        )
        return ConversationHandler.END

def salary_enter_amount(update, context):
    try:
        if update.message.text.startswith('/'):  # 如果是命令，结束对话
            context.user_data.clear()
            update.message.reply_text(
                "❌ Operation cancelled due to new command.",
                reply_markup=ReplyKeyboardRemove()
            )
            return ConversationHandler.END
            
        try:
            amount = float(update.message.text)
            if amount <= 0:
                raise ValueError("Salary must be positive")
        except ValueError:
            update.message.reply_text(
                "❌ Please enter a valid positive number.\n"
                "Or use /cancel to cancel this operation.",
                reply_markup=ReplyKeyboardRemove()
            )
            return SALARY_ENTER_AMOUNT
        
        driver_id = context.user_data.get('selected_driver')
        if not driver_id:
            context.user_data.clear()
            update.message.reply_text(
                "❌ Session expired. Please try /salary command again.",
                reply_markup=ReplyKeyboardRemove()
            )
            return ConversationHandler.END
        
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE drivers SET monthly_salary = %s WHERE user_id = %s",
                    (amount, driver_id)
                )
                conn.commit()
        finally:
            release_db_connection(conn)
        
        hourly_rate = calculate_hourly_rate(amount)
        update.message.reply_text(
            f"✅ Salary set to RM{amount:.2f}/month\n"
            f"Hourly rate: RM{hourly_rate:.2f}",
            reply_markup=ReplyKeyboardRemove()
        )
        
        # 清理状态
        context.user_data.clear()
        return ConversationHandler.END
    except Exception as e:
        logger.error(f"Error in salary_enter_amount: {str(e)}")
        context.user_data.clear()  # 确保清理状态
        update.message.reply_text(
            "❌ An error occurred. Please try /salary command again.",
            reply_markup=ReplyKeyboardRemove()
        )
        return ConversationHandler.END

def cancel(update, context):
    """取消当前操作"""
    try:
        # 清理状态
        context.user_data.clear()
        update.message.reply_text(
            "❌ Operation cancelled",
            reply_markup=ReplyKeyboardRemove()
        )
    except Exception as e:
        logger.error(f"Error in cancel: {str(e)}")
        update.message.reply_text(
            "❌ An error occurred while cancelling.",
            reply_markup=ReplyKeyboardRemove()
        )
    return ConversationHandler.END

def error_handler(update, context):
    logger.error("Exception while handling an update:", exc_info=context.error)
    
    try:
        if update and update.effective_message:
            update.effective_message.reply_text(
                "⚠️ An unexpected error occurred. Please try again later."
            )
    except:
        logger.error("Failed to send error message to user")
    
    tb_list = traceback.format_exception(None, context.error, context.error.__traceback__)
    tb_string = ''.join(tb_list)
    logger.error(f"Full traceback:\n{tb_string}")

# === Webhook ===
@app.route("/webhook", methods=["POST"])
def webhook():
    try:
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

# === 初始化数据库和处理器 ===
init_db()

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

# === 时间处理工具更新 ===
def get_user_timezone(user_id):
    """获取用户的时区设置"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT timezone FROM drivers WHERE user_id = %s", (user_id,))
            result = cur.fetchone()
            return result[0] if result else DEFAULT_TIMEZONE
    finally:
        release_db_connection(conn)

def get_current_time_for_user(user_id):
    """获取用户所在时区的当前时间"""
    timezone = get_user_timezone(user_id)
    return datetime.datetime.now(pytz.timezone(timezone))

def get_current_date_for_user(user_id):
    """获取用户所在时区的当前日期"""
    return get_current_time_for_user(user_id).date()

# === 更新 init_bot 函数 ===
def init_bot():
    """初始化 Telegram Bot 和 Dispatcher"""
    global dispatcher
    dispatcher = Dispatcher(bot, None, use_context=True)
    
    # 注册基本命令处理器
    dispatcher.add_handler(CommandHandler("start", start))
    dispatcher.add_handler(CommandHandler("clockin", clockin))
    dispatcher.add_handler(CommandHandler("clockout", clockout))
    dispatcher.add_handler(CommandHandler("offday", offday))
    dispatcher.add_handler(CommandHandler("balance", balance))
    dispatcher.add_handler(CommandHandler("check", check))
    dispatcher.add_handler(CommandHandler("viewclaims", viewclaims))
    dispatcher.add_handler(CommandHandler("PDF", pdf_start))
    dispatcher.add_handler(CallbackQueryHandler(pdf_button_callback, pattern=r'^all|\d+$'))

    # 添加位置处理器
    dispatcher.add_handler(MessageHandler(Filters.location, handle_location))
    
    # 添加打卡后文本消息处理器（处理拒绝位置的情况）
    dispatcher.add_handler(MessageHandler(
        Filters.text & ~Filters.command, 
        handle_text_after_clockin
    ))

    # 添加 PAID 命令处理程序
    dispatcher.add_handler(ConversationHandler(
        entry_points=[CommandHandler("paid", paid_start)],
        states={
            PAID_SELECT_DRIVER: [MessageHandler(Filters.text & ~Filters.command, paid_select_driver)],
            PAID_START_DATE: [MessageHandler(Filters.text & ~Filters.command, paid_start_date)],
            PAID_END_DATE: [MessageHandler(Filters.text & ~Filters.command, paid_end_date)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    ))

    # 其他对话处理器
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

    # 注册错误处理器
    dispatcher.add_error_handler(error_handler)
    
    logger.info("Bot handlers initialized successfully")

def calculate_work_summary(user_id):
    """计算员工工作统计"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # 获取员工信息
            cur.execute("""
                SELECT first_name, username, monthly_salary, total_hours 
                FROM drivers 
                WHERE user_id = %s
            """, (user_id,))
            driver = cur.fetchone()
            
            if not driver:
                return None
                
            # 计算工作天数
            cur.execute("""
                SELECT COUNT(*) 
                FROM clock_logs 
                WHERE user_id = %s 
                AND clock_in IS NOT NULL 
                AND clock_out IS NOT NULL
            """, (user_id,))
            total_days = cur.fetchone()[0]
            
            # 计算总工时和工资
            first_name, username, monthly_salary, total_hours = driver
            hourly_rate = calculate_hourly_rate(monthly_salary)
            total_salary = total_hours * hourly_rate
            
            return {
                'name': f"@{username}" if username else first_name,
                'total_days': total_days,
                'total_hours': total_hours,
                'hourly_rate': hourly_rate,
                'total_salary': total_salary
            }
    finally:
        release_db_connection(conn)

def calculate_work_summary_with_date_range(user_id, start_date, end_date):
    """计算指定日期范围内的员工工作统计"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # 获取员工信息
            cur.execute("""
                SELECT first_name, username, monthly_salary
                FROM drivers 
                WHERE user_id = %s
            """, (user_id,))
            driver = cur.fetchone()
            
            if not driver:
                return None
            
            # 计算指定日期范围内的工作天数和工时
            cur.execute("""
                SELECT 
                    COUNT(*) as total_days,
                    COALESCE(
                        SUM(
                            CASE 
                                WHEN clock_in IS NOT NULL AND clock_out IS NOT NULL 
                                THEN EXTRACT(EPOCH FROM (clock_out::timestamp - clock_in::timestamp)) / 3600.0
                                ELSE 0 
                            END
                        ),
                        0
                    ) as total_hours
                FROM clock_logs 
                WHERE user_id = %s 
                AND date BETWEEN %s AND %s
                AND NOT is_off
                AND clock_in IS NOT NULL 
                AND clock_out IS NOT NULL
            """, (user_id, start_date, end_date))
            
            total_days, total_hours = cur.fetchone()
            total_days = total_days or 0
            total_hours = float(total_hours or 0)
            
            # 获取员工信息
            first_name, username, monthly_salary = driver
            hourly_rate = calculate_hourly_rate(monthly_salary)
            total_salary = total_hours * hourly_rate
            
            return {
                'name': f"@{username}" if username else first_name,
                'total_days': total_days,
                'total_hours': total_hours,
                'hourly_rate': hourly_rate,
                'total_salary': total_salary,
                'start_date': start_date,
                'end_date': end_date
            }
    except Exception as e:
        logger.error(f"Error in calculate_work_summary_with_date_range: {str(e)}")
        return None
    finally:
        release_db_connection(conn)

def validate_date(date_str):
    """验证日期格式 (DD/MM/YYYY)"""
    try:
        return datetime.datetime.strptime(date_str, "%d/%m/%Y").date()
    except ValueError:
        return None

def paid_start(update, context):
    """开始PAID命令处理"""
    try:
        if update.effective_user.id not in ADMIN_IDS:
            return
        
        # 清理之前可能存在的状态
        context.user_data.clear()
        
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT d.user_id, d.first_name, d.username, d.monthly_salary 
                    FROM drivers d
                """)
                drivers = cur.fetchall()
        finally:
            release_db_connection(conn)
        
        # 过滤掉未设置月薪的员工
        valid_drivers = [d for d in drivers if d[3] is not None and d[3] > 0]
        
        if not valid_drivers:
            update.message.reply_text(
                "❌ No drivers found with salary set.\n"
                "Please set salary first using /salary command."
            )
            return ConversationHandler.END
        
        keyboard = [[f"{d[1]} (ID: {d[0]}) - RM{d[3]:.2f}/month"] for d in valid_drivers]
        context.user_data['paid_drivers'] = {f"{d[1]} (ID: {d[0]}) - RM{d[3]:.2f}/month": d[0] for d in valid_drivers}
        
        update.message.reply_text(
            "👤 Select driver to view payment summary:",
            reply_markup=ReplyKeyboardMarkup(keyboard, one_time_keyboard=True)
        )
        return PAID_SELECT_DRIVER
    except Exception as e:
        logger.error(f"Error in paid_start: {str(e)}")
        update.message.reply_text(
            "❌ An error occurred. Please try /paid command again.",
            reply_markup=ReplyKeyboardRemove()
        )
        return ConversationHandler.END

def paid_select_driver(update, context):
    """处理PAID命令选择的员工"""
    try:
        selected = update.message.text
        drivers = context.user_data.get('paid_drivers', {})
        
        if selected not in drivers:
            update.message.reply_text(
                "❌ Invalid selection.",
                reply_markup=ReplyKeyboardRemove()
            )
            return ConversationHandler.END
        
        context.user_data['selected_driver_id'] = drivers[selected]
        update.message.reply_text(
            "📅 Enter start date (DD/MM/YYYY):",
            reply_markup=ReplyKeyboardRemove()
        )
        return PAID_START_DATE
    except Exception as e:
        logger.error(f"Error in paid_select_driver: {str(e)}")
        update.message.reply_text(
            "❌ An error occurred. Please try /paid command again.",
            reply_markup=ReplyKeyboardRemove()
        )
        return ConversationHandler.END

def paid_start_date(update, context):
    """处理开始日期输入"""
    try:
        start_date = validate_date(update.message.text)
        if not start_date:
            update.message.reply_text(
                "❌ Invalid date format. Please use DD/MM/YYYY\n"
                "Example: 01/03/2024"
            )
            return PAID_START_DATE
        
        context.user_data['start_date'] = start_date
        update.message.reply_text("📅 Enter end date (DD/MM/YYYY):")
        return PAID_END_DATE
    except Exception as e:
        logger.error(f"Error in paid_start_date: {str(e)}")
        update.message.reply_text(
            "❌ An error occurred. Please try /paid command again.",
            reply_markup=ReplyKeyboardRemove()
        )
        return ConversationHandler.END

def paid_end_date(update, context):
    """处理结束日期输入并显示结果"""
    try:
        end_date = validate_date(update.message.text)
        if not end_date:
            update.message.reply_text(
                "❌ Invalid date format. Please use DD/MM/YYYY\n"
                "Example: 31/03/2024"
            )
            return PAID_END_DATE
        
        start_date = context.user_data.get('start_date')
        if end_date < start_date:
            update.message.reply_text("❌ End date must be after start date.")
            return PAID_END_DATE
        
        driver_id = context.user_data.get('selected_driver_id')
        summary = calculate_work_summary_with_date_range(driver_id, start_date, end_date)
        
        if not summary:
            update.message.reply_text(
                "❌ Failed to calculate summary.",
                reply_markup=ReplyKeyboardRemove()
            )
            return ConversationHandler.END
        
        message = (
            f"📊 Payment Summary for {summary['name']}\n\n"
            f"📅 Period: {summary['start_date'].strftime('%d/%m/%Y')} - "
            f"{summary['end_date'].strftime('%d/%m/%Y')}\n\n"
            f"🗓 Total Working Days: {summary['total_days']}\n"
            f"⏰ Total Hours: {format_duration(summary['total_hours'])}\n"
            f"💰 Hourly Rate: RM{summary['hourly_rate']:.2f}\n"
            f"💵 Total Salary: RM{summary['total_salary']:.2f}"
        )
        
        update.message.reply_text(
            message,
            reply_markup=ReplyKeyboardRemove()
        )
        # 清理状态
        context.user_data.clear()
        return ConversationHandler.END
    except Exception as e:
        logger.error(f"Error in paid_end_date: {str(e)}")
        update.message.reply_text(
            "❌ An error occurred. Please try /paid command again.",
            reply_markup=ReplyKeyboardRemove()
        )
        return ConversationHandler.END

def get_address_from_location(latitude, longitude):
    """根据经纬度获取地址"""
    try:
        url = f"https://maps.googleapis.com/maps/api/geocode/json?latlng={latitude},{longitude}&key={os.getenv('GOOGLE_API_KEY')}"
        response = requests.get(url)
        data = response.json()
        
        if data['status'] == 'OK' and data['results']:
            # 获取最精确的地址
            return data['results'][0]['formatted_address']
        else:
            logger.error(f"Error getting address: {data}")
            return "Address not available"
    except Exception as e:
        logger.error(f"Error in get_address_from_location: {e}")
        return "Address lookup failed"
