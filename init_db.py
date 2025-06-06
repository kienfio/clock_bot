import os
import psycopg2
import psycopg2.extras
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import logging
import requests

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# === 添加地址解析功能 ===
def get_address_from_location(latitude, longitude):
    """根据经纬度获取地址"""
    try:
        # 从环境变量获取API密钥
        GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY")
        if not GOOGLE_API_KEY:
            logger.error("GOOGLE_API_KEY not set in environment variables")
            return "API key not available"
            
        url = f"https://maps.googleapis.com/maps/api/geocode/json?latlng={latitude},{longitude}&key={GOOGLE_API_KEY}"
        response = requests.get(url, timeout=5)
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

def init_database():
    """初始化数据库和表结构"""
    # 从环境变量获取数据库连接信息
    DATABASE_URL = os.environ.get("DATABASE_URL")
    
    if not DATABASE_URL:
        raise ValueError("需要设置 DATABASE_URL 环境变量")
    
    logger.info("开始初始化数据库...")
    
    try:
        # 连接数据库
        conn = psycopg2.connect(
            DATABASE_URL,
            cursor_factory=psycopg2.extras.DictCursor
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        
        # 设置时区
        cur.execute("SET timezone TO 'Asia/Kuala_Lumpur'")
        
        # 创建表
        # 1. 司机表
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
        logger.info("创建 drivers 表成功")
        
        # 2. 打卡记录表
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
        logger.info("创建 clock_logs 表成功")

        # 3. 月度报告表
        cur.execute("""
        CREATE TABLE IF NOT EXISTS monthly_reports (
            id SERIAL PRIMARY KEY,
            user_id BIGINT REFERENCES drivers(user_id),
            report_date DATE NOT NULL,
            total_claims FLOAT DEFAULT 0.0,
            total_ot_hours FLOAT DEFAULT 0.0,
            total_salary FLOAT DEFAULT 0.0,
            work_days INTEGER DEFAULT 0,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(user_id, report_date)
        )
        """)
        logger.info("创建 monthly_reports 表成功")

        # 4. Claims表（如果还没有的话）
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
        logger.info("创建 claims 表成功")
        
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
        
        # 创建索引
        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_clock_logs_user_date ON clock_logs(user_id, date);
        CREATE INDEX IF NOT EXISTS idx_monthly_reports_user_date ON monthly_reports(user_id, report_date);
        CREATE INDEX IF NOT EXISTS idx_claims_user_date ON claims(user_id, date);
        """)
        logger.info("创建索引成功")
        
        # 关闭连接
        cur.close()
        conn.close()
        logger.info("数据库初始化完成！")
        
    except Exception as e:
        logger.error(f"数据库初始化失败: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        init_database()
    except Exception as e:
        logger.error(f"程序执行失败: {str(e)}")
        exit(1) 
