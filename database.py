import psycopg2
import psycopg2.extras
from psycopg2 import pool
import logging
import time
import atexit
import os
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

logger = logging.getLogger(__name__)

# 数据库连接池
db_pool = None

def init_db():
    """初始化数据库连接池"""
    global db_pool
    try:
        if db_pool is None:
            DATABASE_URL = os.getenv('DATABASE_URL')
            db_pool = psycopg2.pool.SimpleConnectionPool(
                1,  # 最小连接数
                10, # 最大连接数
                DATABASE_URL
            )
            logger.info("Database connection pool initialized successfully")
    except Exception as e:
        logger.error(f"Error initializing database pool: {e}")
        raise

def get_db_connection():
    """获取数据库连接"""
    try:
        conn = db_pool.getconn()
        return conn
    except psycopg2.pool.PoolError:
        logger.error("Connection pool exhausted, waiting for available connection...")
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

# 注册应用退出时的清理函数
atexit.register(close_all_db_connections)

# 数据库操作函数
def get_user_claims(user_id, start_date=None, end_date=None):
    """获取用户的报销记录"""
    conn = get_db_connection()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        query = "SELECT * FROM claims WHERE user_id = %s"
        params = [user_id]
        
        if start_date and end_date:
            query += " AND created_at BETWEEN %s AND %s"
            params.extend([start_date, end_date])
        
        cur.execute(query, params)
        return cur.fetchall()
    finally:
        if 'cur' in locals():
            cur.close()
        release_db_connection(conn)

def get_user_clock_records(user_id, start_date=None, end_date=None):
    """获取用户的打卡记录"""
    conn = get_db_connection()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        query = "SELECT * FROM clock_records WHERE user_id = %s"
        params = [user_id]
        
        if start_date and end_date:
            query += " AND clock_in BETWEEN %s AND %s"
            params.extend([start_date, end_date])
        
        cur.execute(query, params)
        return cur.fetchall()
    finally:
        if 'cur' in locals():
            cur.close()
        release_db_connection(conn)

def update_claim_status(claim_id, status):
    """更新报销申请状态"""
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE claims SET status = %s WHERE id = %s",
            (status, claim_id)
        )
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Error updating claim status: {e}")
        return False
    finally:
        if 'cur' in locals():
            cur.close()
        release_db_connection(conn)

def get_monthly_summary(user_id, year, month):
    """获取用户月度汇总数据"""
    conn = get_db_connection()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        # 获取该月的起始和结束日期
        start_date = f"{year}-{month:02d}-01"
        if month == 12:
            end_date = f"{year + 1}-01-01"
        else:
            end_date = f"{year}-{month + 1:02d}-01"
            
        # 获取工作记录
        cur.execute("""
            SELECT 
                COUNT(DISTINCT DATE(clock_in)) as work_days,
                SUM(EXTRACT(EPOCH FROM (clock_out - clock_in))/3600) as total_hours
            FROM clock_records 
            WHERE user_id = %s 
            AND clock_in >= %s 
            AND clock_in < %s
            AND clock_out IS NOT NULL
        """, (user_id, start_date, end_date))
        
        work_stats = cur.fetchone()
        
        # 获取报销金额
        cur.execute("""
            SELECT COALESCE(SUM(amount), 0) as total_claims
            FROM claims
            WHERE user_id = %s 
            AND created_at >= %s 
            AND created_at < %s
            AND status = 'PAID'
        """, (user_id, start_date, end_date))
        
        claims = cur.fetchone()
        
        # 获取休息日数量
        cur.execute("""
            SELECT COUNT(*) as off_days
            FROM offday_records
            WHERE user_id = %s 
            AND date >= %s 
            AND date < %s
        """, (user_id, start_date, end_date))
        
        off_days = cur.fetchone()
        
        return {
            'work_days': work_stats['work_days'] or 0,
            'total_hours': work_stats['total_hours'] or 0,
            'total_claims': claims['total_claims'] or 0,
            'off_days': off_days['off_days'] or 0
        }
        
    finally:
        if 'cur' in locals():
            cur.close()
        release_db_connection(conn) 
