
import logging
import datetime
import pytz
from telegram import ReplyKeyboardMarkup, ReplyKeyboardRemove, InlineKeyboardButton, InlineKeyboardMarkup
from database import (
    get_db_connection, 
    release_db_connection,
    get_user_claims,
    get_user_clock_records,
    update_claim_status,
    get_monthly_summary
)

logger = logging.getLogger(__name__)

# 状态常量
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
PREVIOUSREPORT_SELECT_MONTH = 20

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
                "🟢 /paid\n"
                "📈 /previousreport"
            )
    except Exception as e:
        logger.error(f"Error in start command: {str(e)}")
        welcome_msg = "❌ An error occurred. Please try again or contact admin."
    finally:
        release_db_connection(conn)
    
    update.message.reply_text(welcome_msg)

def clockin(update, context):
    """处理打卡命令"""
    user = update.effective_user
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        
        # 检查是否已经打卡
        cur.execute(
            """SELECT id FROM clock_records 
               WHERE user_id = %s AND clock_out IS NULL""",
            (user.id,)
        )
        
        if cur.fetchone():
            update.message.reply_text("You have already clocked in.")
            return
            
        # 请求位置
        keyboard = [[KeyboardButton(text="Send Location 📍", request_location=True)]]
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True)
        update.message.reply_text(
            "Please share your location:",
            reply_markup=reply_markup
        )
        return "WAITING_LOCATION"
        
    except Exception as e:
        logger.error(f"Error in clockin: {str(e)}")
        update.message.reply_text("An error occurred. Please try again.")
    finally:
        if 'cur' in locals():
            cur.close()
        release_db_connection(conn)

def clockout(update, context):
    """处理下班打卡命令"""
    user = update.effective_user
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        now = datetime.datetime.now(pytz.timezone('Asia/Kuala_Lumpur'))
        
        # 获取最近的打卡记录
        cur.execute(
            """SELECT id FROM clock_records 
               WHERE user_id = %s AND clock_out IS NULL
               ORDER BY clock_in DESC LIMIT 1""",
            (user.id,)
        )
        record = cur.fetchone()
        
        if not record:
            update.message.reply_text("You haven't clocked in yet.")
            return
            
        # 更新打卡记录
        cur.execute(
            """UPDATE clock_records SET clock_out = %s 
               WHERE id = %s""",
            (now, record[0])
        )
        conn.commit()
        update.message.reply_text("Clock out successful! Have a great rest! 😊")
        
    except Exception as e:
        logger.error(f"Error in clockout: {str(e)}")
        update.message.reply_text("An error occurred. Please try again.")
    finally:
        if 'cur' in locals():
            cur.close()
        release_db_connection(conn)

def previousreport_start(update, context):
    """开始查看历史报告流程"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        update.message.reply_text("Sorry, this command is only available for administrators.")
        return ConversationHandler.END

    # 获取当前月份和年份
    current_date = datetime.datetime.now()
    months = []
    # 获取过去6个月
    for i in range(1, 7):
        past_date = current_date - datetime.timedelta(days=30*i)
        months.append((past_date.strftime("%B %Y"), past_date.strftime("%Y-%m")))

    keyboard = []
    for month_name, month_value in months:
        keyboard.append([InlineKeyboardButton(month_name, callback_data=f"prevreport_{month_value}")])

    reply_markup = InlineKeyboardMarkup(keyboard)
    update.message.reply_text(
        "Please select the month you want to view the report for:",
        reply_markup=reply_markup
    )
    return PREVIOUSREPORT_SELECT_MONTH

def previousreport_month_selected(update, context):
    """处理月份选择，生成报告"""
    query = update.callback_query
    query.answer()
    
    # 解析选择的月份和年份
    _, selected_date = query.data.split('_')
    year, month = map(int, selected_date.split('-'))
    
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        # 获取所有在该月工作过的员工
        start_date = f"{year}-{month:02d}-01"
        if month == 12:
            end_date = f"{year + 1}-01-01"
        else:
            end_date = f"{year}-{month + 1:02d}-01"
            
        cur.execute("""
            SELECT DISTINCT u.user_id, u.username, u.first_name, u.last_name
            FROM drivers u
            JOIN clock_records c ON u.user_id = c.user_id
            WHERE c.clock_in >= %s AND c.clock_in < %s
        """, (start_date, end_date))
        
        workers = cur.fetchall()
        
        if not workers:
            query.edit_message_text("No work records found for the selected month.")
            return ConversationHandler.END
        
        report_text = f"📊 Monthly Report for {datetime.datetime(year, month, 1).strftime('%B %Y')}\n\n"
        
        for worker in workers:
            # 获取月度汇总数据
            summary = get_monthly_summary(worker['user_id'], year, month)
            
            # 格式化工作时长
            total_hours = summary['total_hours']
            hours = int(total_hours)
            minutes = int((total_hours - hours) * 60)
            
            # 添加员工统计到报告
            worker_name = worker['first_name'] or worker['username'] or f"User {worker['user_id']}"
            report_text += f"👤 {worker_name}:\n"
            report_text += f"   • Work Days: {summary['work_days']}\n"
            report_text += f"   • Total Hours: {hours}h {minutes}m\n"
            report_text += f"   • Off Days: {summary['off_days']}\n"
            report_text += f"   • Claims Paid: ${summary['total_claims']:.2f}\n\n"
        
        # 如果报告太长，分段发送
        if len(report_text) > 4000:
            for i in range(0, len(report_text), 4000):
                if i == 0:
                    query.edit_message_text(report_text[i:i+4000])
                else:
                    context.bot.send_message(
                        chat_id=update.effective_chat.id,
                        text=report_text[i:i+4000]
                    )
        else:
            query.edit_message_text(report_text)
        
    except Exception as e:
        logger.error(f"Error generating previous report: {str(e)}")
        query.edit_message_text("An error occurred while generating the report. Please try again later.")
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            release_db_connection(conn)
    
    return ConversationHandler.END 
