import time
import random
import logging
import sys
import os
import sqlite3

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)

def main():
    logging.info("示例爬虫开始运行")
    print("print示例输出")
    
    # 获取当前目录
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # 连接SQLite数据库
    db_path = os.path.join(current_dir, 'crawler_data.db')
    logging.info(f"数据库路径: {db_path}")
    
    # 创建数据库连接
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 创建表（如果不存在）
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS crawler_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        url TEXT NOT NULL,
        content TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    conn.commit()
    
    # 模拟爬虫工作
    for i in range(10):
        logging.info(f"正在处理第 {i+1} 个任务")
        time.sleep(random.uniform(0.5, 2))
        
        # 随机模拟一些错误
        if random.random() < 0.2:
            logging.warning(f"处理第 {i+1} 个任务时遇到警告")
        
        # 随机添加一些数据到数据库
        if random.random() > 0.5:
            title = f"爬取的标题 {i+1}"
            url = f"https://example.com/page{i+1}"
            content = f"这是第 {i+1} 个爬取的内容，包含一些随机文本: {random.randint(1000, 9999)}"
            
            try:
                cursor.execute(
                    "INSERT INTO crawler_data (title, url, content) VALUES (?, ?, ?)",
                    (title, url, content)
                )
                conn.commit()
                logging.info(f"已保存数据: {title}")
            except Exception as e:
                logging.error(f"保存数据失败: {str(e)}")
    
    # 关闭数据库连接
    conn.close()
    logging.info("示例爬虫运行完成")

if __name__ == "__main__":
    main()