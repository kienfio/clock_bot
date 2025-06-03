import os
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import logging

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def init_database():
    """初始化数据库和表结构"""
    # 从环境变量获取数据库连接信息
    DATABASE_URL = os.environ.get("DATABASE_URL")
    
    if not DATABASE_URL:
        raise ValueError("需要设置 DATABASE_URL 环境变量")
    
    logger.info("开始初始化数据库...")
    
    try:
        # 连接数据库
        conn = psycopg2.connect(DATABASE_URL)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        
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
            clock_in TIMESTAMP WITH TIME ZONE,
            clock_out TIMESTAMP WITH TIME ZONE,
            is_off BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(user_id, date)
        )
        """)
        logger.info("创建 clock_logs 表成功")
        
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
        # 为常用查询创建索引以提高性能
        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_clock_logs_user_date ON clock_logs(user_id, date);
        CREATE INDEX IF NOT EXISTS idx_claims_user_date ON claims(user_id, date);
        CREATE INDEX IF NOT EXISTS idx_topups_user_date ON topups(user_id, date);
        """)
        logger.info("创建索引成功")
        
        # 关闭连接
        cur.close()
        conn.close()
        logger.info("数据库初始化完成！")
        
    except Exception as e:
        logger.error(f"数据库初始化失败: {str(e)}")
        raise

def main():
    """主函数"""
    try:
        init_database()
    except Exception as e:
        logger.error(f"程序执行失败: {str(e)}")
        exit(1)

if __name__ == "__main__":
    main() 
