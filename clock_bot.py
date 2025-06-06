"""
Telegram Bot for Employee Clock In/Out Management
"""
import os
import datetime
import logging
import tempfile
import traceback
from flask import Flask, request, jsonify
import psycopg2
import psycopg2.pool
import psycopg2.extras
import pytz
import requests
import io
import json
from telegram import (
    Update, ParseMode, ReplyKeyboardMarkup, ReplyKeyboardRemove, 
    InlineKeyboardButton, InlineKeyboardMarkup, KeyboardButton
)
from telegram.ext import (
    Updater, CommandHandler, MessageHandler, Filters, 
    CallbackContext, ConversationHandler, CallbackQueryHandler
)
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib import colors

# 设置日志记录
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger('clock_bot')

# 初始化Flask应用
app = Flask(__name__)

# 设置Telegram Bot Token和Admin ID（从环境变量获取）
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
ADMIN_IDS = []
admin_ids_str = os.environ.get("ADMIN_IDS", "")
if admin_ids_str:
    try:
        ADMIN_IDS = [int(x.strip()) for x in admin_ids_str.split(",")]
        logger.info(f"Admin IDs loaded: {ADMIN_IDS}")
    except Exception as e:
        logger.error(f"Error parsing ADMIN_IDS: {e}")

# 创建全局变量存储Telegram Bot实例
bot = None
updater = None

# 创建数据库连接池
db_pool = None

# === 对话状态定义 ===
SALARY_SELECT_DRIVER, SALARY_ENTER_AMOUNT, SALARY_CONFIRM = range(3)
CLAIM_TYPE, CLAIM_OTHER_TYPE, CLAIM_AMOUNT, CLAIM_PROOF = range(4)
VIEWCLAIMS_SELECT_USER, VIEWCLAIMS_SELECT_MONTH = range(2)
CHECKSTATE_SELECT_USER = range(1)
PAID_SELECT_DRIVER, PAID_CONFIRM = range(2)
PREVIOUSREPORT_SELECT_WORKER, PREVIOUSREPORT_SELECT_YEAR, PREVIOUSREPORT_SELECT_MONTH = range(3)

# === 数据库连接管理 ===
def get_db_connection():
    """获取数据库连接"""
    global db_pool
    if db_pool is None:
        init_db()
    try:
        conn = db_pool.getconn()
        # 设置会话时区
        with conn.cursor() as cur:
            cur.execute("SET timezone TO 'Asia/Kuala_Lumpur'")
        conn.autocommit = False
        return conn
    except Exception as e:
        logger.error(f"Error getting database connection: {str(e)}")
        raise

def release_db_connection(conn):
    """释放数据库连接回连接池"""
    global db_pool
    if db_pool and conn:
        db_pool.putconn(conn)

def close_all_db_connections():
    """关闭所有数据库连接（程序结束时调用）"""
    global db_pool
    if db_pool:
        db_pool.closeall()
        logger.info("All database connections closed")

# === Webhook配置 ===
@app.route("/webhook", methods=["POST"])
def webhook():
    """处理来自Telegram的webhook请求"""
    if request.method == "POST":
        try:
            # 解析传入的JSON更新
            update = Update.de_json(request.get_json(force=True), bot)
            
            # 将更新传递给调度器
            updater.dispatcher.process_update(update)
            return "ok", 200
        except Exception as e:
            logger.error(f"Error processing webhook: {e}")
            logger.error(traceback.format_exc())
            return str(e), 500
    return "Method not allowed", 405

@app.route("/health")
def health():
    """健康检查端点"""
    try:
        # 尝试连接数据库
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            result = cur.fetchone()
            assert result[0] == 1
        release_db_connection(conn)
        
        # 检查Telegram Bot状态
        if bot:
            bot_info = bot.get_me()
            return jsonify({
                "status": "healthy",
                "database": "connected",
                "bot": {
                    "id": bot_info.id,
                    "name": bot_info.first_name,
                    "username": bot_info.username
                },
                "admin_ids": ADMIN_IDS
            })
        else:
            return jsonify({
                "status": "degraded",
                "database": "connected",
                "bot": "not initialized"
            })
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        return jsonify({
            "status": "unhealthy",
            "error": str(e)
        }), 500

@app.route("/webhook-status")
def webhook_status():
    """检查webhook状态"""
    try:
        if not bot:
            return jsonify({"error": "Bot not initialized"}), 500
        
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
                    paid BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(user_id, date)
                )
                """)
                
                # 确保 clock_logs 表中的 paid 列存在
                cur.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 
                        FROM information_schema.columns 
                        WHERE table_name='clock_logs' AND column_name='paid'
                    ) THEN
                        ALTER TABLE clock_logs ADD COLUMN paid BOOLEAN DEFAULT FALSE;
                    END IF;
                END $$;
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
                    paid BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                )
                """)
                
                # 确保 ot_logs 表中的 paid 列存在
                cur.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 
                        FROM information_schema.columns 
                        WHERE table_name='ot_logs' AND column_name='paid'
                    ) THEN
                        ALTER TABLE ot_logs ADD COLUMN paid BOOLEAN DEFAULT FALSE;
                    END IF;
                END $$;
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

def init_bot():
    """初始化Telegram Bot和处理器"""
    global bot, updater
    
    if not TELEGRAM_TOKEN:
        logger.error("TELEGRAM_TOKEN not set in environment variables")
        raise ValueError("TELEGRAM_TOKEN is required")
    
    try:
        # 创建Updater和Dispatcher
        updater = Updater(TELEGRAM_TOKEN, use_context=True)
        bot = updater.bot
        dp = updater.dispatcher
        
        # 添加基本命令处理器
        dp.add_handler(CommandHandler("start", start))
        dp.add_handler(CommandHandler("check", check))
        dp.add_handler(CommandHandler("offday", offday))
        dp.add_handler(CommandHandler("clockin", clockin))
        dp.add_handler(CommandHandler("clockout", clockout))
        dp.add_handler(CommandHandler("balance", balance))
        dp.add_handler(CommandHandler("ot", ot))
        dp.add_handler(CommandHandler("pdf", pdf_start))
        
        # 添加按钮回调处理器
        dp.add_handler(CallbackQueryHandler(pdf_button_callback, pattern=r"^pdf_"))
        
        # 添加位置处理器
        dp.add_handler(MessageHandler(Filters.location, handle_location))
        
        # 1. 历史报告对话处理器
        previous_report_conv = ConversationHandler(
            entry_points=[CommandHandler("previousreport", previousreport_start)],
            states={
                PREVIOUSREPORT_SELECT_WORKER: [MessageHandler(Filters.text & ~Filters.command, previousreport_select_worker)],
                PREVIOUSREPORT_SELECT_YEAR: [MessageHandler(Filters.text & ~Filters.command, previousreport_select_year)],
                PREVIOUSREPORT_SELECT_MONTH: [MessageHandler(Filters.text & ~Filters.command, previousreport_select_month)],
            },
            fallbacks=[CommandHandler("cancel", cancel)],
        )
        dp.add_handler(previous_report_conv)
        
        # 工资设置对话处理器
        salary_conv = ConversationHandler(
            entry_points=[CommandHandler("salary", salary_start)],
            states={
                SALARY_SELECT_DRIVER: [MessageHandler(Filters.text & ~Filters.command, salary_select_driver)],
                SALARY_ENTER_AMOUNT: [MessageHandler(Filters.text & ~Filters.command, salary_enter_amount)],
                SALARY_CONFIRM: [MessageHandler(Filters.text & ~Filters.command, salary_confirm)],
            },
            fallbacks=[CommandHandler("cancel", cancel)],
        )
        dp.add_handler(salary_conv)
        
        # 报销申请对话处理器
        claim_conv = ConversationHandler(
            entry_points=[CommandHandler("claim", claim_start)],
            states={
                CLAIM_TYPE: [MessageHandler(Filters.text & ~Filters.command, claim_type)],
                CLAIM_OTHER_TYPE: [MessageHandler(Filters.text & ~Filters.command, claim_other_type)],
                CLAIM_AMOUNT: [MessageHandler(Filters.text & ~Filters.command, claim_amount)],
                CLAIM_PROOF: [MessageHandler(Filters.photo, claim_proof)],
            },
            fallbacks=[CommandHandler("cancel", cancel)],
        )
        dp.add_handler(claim_conv)
        
        # 查看报销记录对话处理器
        viewclaims_conv = ConversationHandler(
            entry_points=[CommandHandler("viewclaims", viewclaims_start)],
            states={
                VIEWCLAIMS_SELECT_USER: [MessageHandler(Filters.text & ~Filters.command, viewclaims_select_user)],
                VIEWCLAIMS_SELECT_MONTH: [MessageHandler(Filters.text & ~Filters.command, viewclaims_select_month)],
            },
            fallbacks=[CommandHandler("cancel", cancel)],
        )
        dp.add_handler(viewclaims_conv)
        
        # 查看工人状态对话处理器
        checkstate_conv = ConversationHandler(
            entry_points=[CommandHandler("checkstate", checkstate_start)],
            states={
                CHECKSTATE_SELECT_USER: [MessageHandler(Filters.text & ~Filters.command, checkstate_select_user)],
            },
            fallbacks=[CommandHandler("cancel", cancel)],
        )
        dp.add_handler(checkstate_conv)
        
        # 工资支付对话处理器
        paid_conv = ConversationHandler(
            entry_points=[CommandHandler("paid", paid_start)],
            states={
                PAID_SELECT_DRIVER: [MessageHandler(Filters.text & ~Filters.command, paid_select_driver)],
                PAID_CONFIRM: [MessageHandler(Filters.text & ~Filters.command, paid_confirm)],
            },
            fallbacks=[CommandHandler("cancel", cancel)],
        )
        dp.add_handler(paid_conv)
        
        # 添加错误处理器
        dp.add_error_handler(error_handler)
        
        logger.info("Bot initialized successfully")
        
        # 如果设置了WEBHOOK_URL，则设置webhook
        WEBHOOK_URL = os.environ.get("WEBHOOK_URL")
        if WEBHOOK_URL:
            # 从URL中提取基本URL
            base_url = "/".join(WEBHOOK_URL.split("/")[:3])
            logger.info(f"Setting webhook to {WEBHOOK_URL}")
            bot.set_webhook(WEBHOOK_URL)
            logger.info(f"Webhook set to {bot.get_webhook_info().url}")
        else:
            logger.warning("WEBHOOK_URL not set, bot will not receive updates")
            
        return True
        
    except Exception as e:
        logger.error(f"Error initializing bot: {str(e)}")
        raise

# Flask app initialization
if __name__ != "__main__":  # when imported by gunicorn
    try:
        # 初始化Bot
        init_bot()
        logger.info("Bot initialized by gunicorn")
    except Exception as e:
        logger.error(f"Failed to initialize bot: {str(e)}")

# For local testing
if __name__ == "__main__":
    try:
        # 初始化Bot
        init_bot()
        
        # 启动轮询（本地测试用）
        updater.start_polling()
        logger.info("Bot started polling")
        
        # 保持程序运行
        updater.idle()
    except Exception as e:
        logger.error(f"Error running bot: {str(e)}")
    finally:
        # 确保关闭数据库连接
        close_all_db_connections() 
