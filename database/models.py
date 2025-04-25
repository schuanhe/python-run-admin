import sqlite3
import os
import datetime
from flask import current_app, g

def get_db():
    """获取数据库连接"""
    if 'db' not in g:
        g.db = sqlite3.connect(
            current_app.config['DATABASE'],
            detect_types=sqlite3.PARSE_DECLTYPES
        )
        g.db.row_factory = sqlite3.Row
        
        # 确保数据库使用Asia/Shanghai时区
        g.db.execute("PRAGMA timezone='Asia/Shanghai'")
    return g.db

def close_db(e=None):
    """关闭数据库连接"""
    db = g.pop('db', None)
    if db is not None:
        db.close()

def init_db():
    """初始化数据库"""
    db = get_db()
    
    # 创建爬虫运行记录表
    db.execute("""
    CREATE TABLE IF NOT EXISTS crawler_runs (
        id TEXT PRIMARY KEY,
        crawler_id TEXT NOT NULL,
        crawler_name TEXT NOT NULL,
        start_time TIMESTAMP NOT NULL,
        end_time TIMESTAMP,
        status TEXT NOT NULL,
        log_path TEXT NOT NULL,
        run_type TEXT DEFAULT 'manual',
        schedule_id TEXT
    )
    """)
    
    # 创建定时任务表
    db.execute("""
    CREATE TABLE IF NOT EXISTS scheduled_tasks (
        id TEXT PRIMARY KEY,
        crawler_id TEXT NOT NULL,
        crawler_name TEXT NOT NULL,
        schedule_type TEXT NOT NULL,
        time_value TEXT NOT NULL,
        created_at TIMESTAMP NOT NULL
    )
    """)
    
    db.commit()

def add_crawler_run(run_id, crawler_id, crawler_name, status, log_path, run_type='manual', schedule_id=None):
    """添加爬虫运行记录
    
    Args:
        run_id: 运行ID
        crawler_id: 爬虫ID
        crawler_name: 爬虫名称
        status: 状态
        log_path: 日志路径
        run_type: 运行类型，'manual'表示手动运行，'scheduled'表示定时任务运行
        schedule_id: 定时任务ID，仅当run_type为'scheduled'时有效
    """
    import datetime
    import pytz
    
    # 使用Asia/Shanghai时区的当前时间
    now = datetime.datetime.now(pytz.timezone('Asia/Shanghai'))
    
    db = get_db()
    db.execute(
        "INSERT INTO crawler_runs (id, crawler_id, crawler_name, start_time, status, log_path, run_type, schedule_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (run_id, crawler_id, crawler_name, now, status, log_path, run_type, schedule_id)
    )
    db.commit()
    return run_id

def update_crawler_status(run_id, status):
    """更新爬虫状态"""
    import datetime
    import pytz
    
    # 使用Asia/Shanghai时区的当前时间
    now = datetime.datetime.now(pytz.timezone('Asia/Shanghai'))
    
    db = get_db()
    db.execute(
        "UPDATE crawler_runs SET status = ?, end_time = ? WHERE id = ?",
        (status, now, run_id)
    )
    db.commit()

def get_crawler_runs(limit=100):
    """获取爬虫运行记录"""
    db = get_db()
    runs = db.execute(
        "SELECT * FROM crawler_runs ORDER BY start_time DESC LIMIT ?",
        (limit,)
    ).fetchall()
    
    # 将 Row 对象转换为字典
    result = []
    for run in runs:
        result.append({
            'id': run['id'],
            'crawler_id': run['crawler_id'],
            'crawler_name': run['crawler_name'],
            'start_time': run['start_time'],
            'end_time': run['end_time'],
            'status': run['status'],
            'log_path': run['log_path'],
            'run_type': run['run_type'],
            'schedule_id': run['schedule_id']
        })
    
    return result

def get_active_crawlers():
    """获取活动中的爬虫"""
    db = get_db()
    crawlers = db.execute(
        "SELECT * FROM crawler_runs WHERE status = 'running'"
    ).fetchall()
    
    # 将 Row 对象转换为字典
    result = []
    for crawler in crawlers:
        result.append({
            'id': crawler['id'],
            'crawler_id': crawler['crawler_id'],
            'crawler_name': crawler['crawler_name'],
            'start_time': crawler['start_time'],
            'status': crawler['status']
        })
    
    return result

def get_crawler_by_id(run_id):
    """根据ID获取爬虫运行记录"""
    db = get_db()
    run = db.execute(
        "SELECT * FROM crawler_runs WHERE id = ?",
        (run_id,)
    ).fetchone()
    
    if run is None:
        return None
    
    return {
        'id': run['id'],
        'crawler_id': run['crawler_id'],
        'crawler_name': run['crawler_name'],
        'start_time': run['start_time'],
        'end_time': run['end_time'],
        'status': run['status'],
        'log_path': run['log_path'],
        'run_type': run['run_type'],
        'schedule_id': run['schedule_id']
    }

# 定时任务相关函数
def add_scheduled_task(task_id, crawler_id, crawler_name, schedule_type, time_value):
    """添加定时任务记录
    
    Args:
        task_id: 任务ID
        crawler_id: 爬虫ID
        crawler_name: 爬虫名称
        schedule_type: 调度类型，'daily'或'interval'
        time_value: 时间值，daily类型为HH:MM格式，interval类型为小时数
    """
    import datetime
    import pytz
    
    # 使用Asia/Shanghai时区的当前时间
    now = datetime.datetime.now(pytz.timezone('Asia/Shanghai'))
    
    db = get_db()
    db.execute(
        "INSERT INTO scheduled_tasks (id, crawler_id, crawler_name, schedule_type, time_value, created_at) VALUES (?, ?, ?, ?, ?, ?)",
        (task_id, crawler_id, crawler_name, schedule_type, time_value, now)
    )
    db.commit()
    return task_id

def remove_scheduled_task(task_id):
    """删除定时任务记录"""
    db = get_db()
    db.execute("DELETE FROM scheduled_tasks WHERE id = ?", (task_id,))
    db.commit()
    return True

def get_scheduled_tasks():
    """获取所有定时任务记录"""
    db = get_db()
    tasks = db.execute("SELECT * FROM scheduled_tasks").fetchall()
    
    # 将 Row 对象转换为字典
    result = []
    for task in tasks:
        result.append({
            'id': task['id'],
            'crawler_id': task['crawler_id'],
            'crawler_name': task['crawler_name'],
            'schedule_type': task['schedule_type'],
            'time_value': task['time_value'],
            'created_at': task['created_at']
        })
    
    return result

def get_scheduled_task_by_id(task_id):
    """根据ID获取定时任务记录"""
    db = get_db()
    task = db.execute(
        "SELECT * FROM scheduled_tasks WHERE id = ?",
        (task_id,)
    ).fetchone()
    
    if task is None:
        return None
    
    return {
        'id': task['id'],
        'crawler_id': task['crawler_id'],
        'crawler_name': task['crawler_name'],
        'schedule_type': task['schedule_type'],
        'time_value': task['time_value'],
        'created_at': task['created_at']
    }