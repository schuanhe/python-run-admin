import os
import json
import time
import datetime
import subprocess
import threading
import logging
import uuid
import pytz
from pathlib import Path
from database.models import add_crawler_run, update_crawler_status, get_crawler_by_id, add_scheduled_task as db_add_scheduled_task, remove_scheduled_task as db_remove_scheduled_task, get_scheduled_tasks as db_get_scheduled_tasks, get_scheduled_task_by_id
from apscheduler.schedulers.background import BackgroundScheduler
import sys

class CrawlerManager:
    def __init__(self, app):
        if app is None:
            raise ValueError("Flask app instance is required")
            
        self.crawlers_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'crawlers')
        self.logs_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
        self.active_crawlers = {}
        self.scheduled_tasks = {}
        self.scheduler = BackgroundScheduler(timezone=pytz.timezone('Asia/Shanghai'))
        self.scheduler.start()
        self.app = app
        
        # 确保日志目录存在
        os.makedirs(self.logs_dir, exist_ok=True)
        
        # 创建示例爬虫（如果不存在）
        self._create_example_crawler()
        
        # 从数据库加载定时任务
        self._load_scheduled_tasks_from_db()
    
    def _create_example_crawler(self):
        example_dir = os.path.join(self.crawlers_dir, 'example_crawler')
        os.makedirs(example_dir, exist_ok=True)
        
        # 创建main.py
        main_py_path = os.path.join(example_dir, 'main.py')
        if not os.path.exists(main_py_path):
            with open(main_py_path, 'w', encoding='utf-8') as f:
                f.write("""
import time
import random
import logging

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def main():
    logging.info("示例爬虫开始运行")
    
    # 模拟爬虫工作
    for i in range(10):
        logging.info(f"正在处理第 {i+1} 个任务")
        time.sleep(random.uniform(0.5, 2))
        
        # 随机模拟一些错误
        if random.random() < 0.2:
            logging.warning(f"处理第 {i+1} 个任务时遇到警告")
    
    logging.info("示例爬虫运行完成")

if __name__ == "__main__":
    main()
""")
        
        # 创建config.json
        config_path = os.path.join(example_dir, 'config.json')
        if not os.path.exists(config_path):
            with open(config_path, 'w', encoding='utf-8') as f:
                json.dump({
                    "name": "示例爬虫",
                    "description": "这是一个示例爬虫，用于演示系统功能",
                    "version": "1.0",
                    "author": "系统",
                    "parameters": {}
                }, f, ensure_ascii=False, indent=4)
    
    def get_all_crawlers(self):
        """获取所有爬虫信息"""
        crawlers = []
        
        for crawler_dir in os.listdir(self.crawlers_dir):
            crawler_path = os.path.join(self.crawlers_dir, crawler_dir)
            if os.path.isdir(crawler_path):
                config_path = os.path.join(crawler_path, 'config.json')
                main_path = os.path.join(crawler_path, 'main.py')
                
                if os.path.exists(config_path) and os.path.exists(main_path):
                    try:
                        with open(config_path, 'r', encoding='utf-8') as f:
                            config = json.load(f)
                        
                        crawlers.append({
                            'id': crawler_dir,
                            'name': config.get('name', crawler_dir),
                            'description': config.get('description', ''),
                            'version': config.get('version', '1.0'),
                            'author': config.get('author', '未知'),
                            'parameters': config.get('parameters', {})
                        })
                    except Exception as e:
                        logging.error(f"读取爬虫配置失败: {crawler_dir}, 错误: {str(e)}")
        
        return crawlers
    
    def get_crawler_by_id(self, crawler_id):
        """根据ID获取爬虫信息"""
        crawler_path = os.path.join(self.crawlers_dir, crawler_id)
        config_path = os.path.join(crawler_path, 'config.json')
        
        if os.path.exists(config_path):
            try:
                with open(config_path, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                
                return {
                    'id': crawler_id,
                    'name': config.get('name', crawler_id),
                    'description': config.get('description', ''),
                    'version': config.get('version', '1.0'),
                    'author': config.get('author', '未知'),
                    'parameters': config.get('parameters', {})
                }
            except Exception as e:
                logging.error(f"读取爬虫配置失败: {crawler_id}, 错误: {str(e)}")
        
        return None
    
    def run_crawler(self, crawler_id, run_type='manual', schedule_id=None):
        """运行爬虫
        
        Args:
            crawler_id: 爬虫ID
            run_type: 运行类型，'manual'表示手动运行，'scheduled'表示定时任务运行
            schedule_id: 定时任务ID，仅当run_type为'scheduled'时有效
        """
        crawler = self.get_crawler_by_id(crawler_id)
        if not crawler:
            return None
        
        # 创建日志目录
        now = datetime.datetime.now(pytz.timezone('Asia/Shanghai'))
        year_dir = os.path.join(self.logs_dir, str(now.year))
        month_dir = os.path.join(year_dir, str(now.month))
        os.makedirs(month_dir, exist_ok=True)
        
        # 生成日志文件名
        log_filename = f"{now.strftime('%Y-%m-%d %H-%M-%S')}_{crawler['name']}.log"
        log_path = os.path.join(month_dir, log_filename)
        
        # 生成运行ID
        run_id = str(uuid.uuid4())
        
        # 记录到数据库
        with self.app.app_context():
            add_crawler_run(run_id, crawler_id, crawler['name'], 'running', log_path, run_type, schedule_id)
        
        # 启动爬虫线程
        thread = threading.Thread(
            target=self._run_crawler_process,
            args=(run_id, crawler_id, crawler['name'], log_path, self.app)
        )
        thread.daemon = True
        thread.start()
        
        # 记录活动爬虫
        self.active_crawlers[run_id] = {
            'crawler_id': crawler_id,
            'name': crawler['name'],
            'start_time': now.strftime('%Y-%m-%d %H:%M:%S'),  # 已使用Asia/Shanghai时区的now
            'thread': thread,
            'run_type': run_type,
            'schedule_id': schedule_id
        }
        
        return run_id
    
    def _run_crawler_process(self, run_id, crawler_id, crawler_name, log_path, app, timeout=3600):
        """在单独的进程中运行爬虫
        
        Args:
            run_id: 运行ID
            crawler_id: 爬虫ID
            crawler_name: 爬虫名称
            log_path: 日志文件路径
            app: Flask应用实例
            timeout: 超时时间(秒)，默认1小时
        """
        try:
            # 爬虫路径
            crawler_path = os.path.join(self.crawlers_dir, crawler_id)
            main_script = os.path.join(crawler_path, 'main.py')

            # 确保日志目录存在
            os.makedirs(os.path.dirname(log_path), exist_ok=True)

            # 运行爬虫进程
            with open(log_path, 'w', encoding='utf-8') as log_file:
                # 使用subprocess.Popen运行爬虫，并将输出重定向到日志文件
                process = subprocess.Popen(
                    [sys.executable, main_script],  # 使用sys.executable确保使用正确的Python解释器
                    stdout=subprocess.PIPE,  # 捕获标准输出
                    stderr=subprocess.PIPE,  # 捕获标准错误
                    cwd=crawler_path,
                    env=os.environ.copy()  # 复制当前环境变量
                )

                try:
                    # 实时处理标准输出和标准错误
                    for stdout_line in iter(process.stdout.readline, b''):  # 实时读取标准输出
                        log_file.write(stdout_line.decode('utf-8'))  # 将标准输出写入日志文件
                        log_file.flush()  # 确保实时写入磁盘
                    for stderr_line in iter(process.stderr.readline, b''):  # 实时读取标准错误
                        log_file.write(stderr_line.decode('utf-8'))  # 将标准错误写入日志文件
                        log_file.flush()  # 确保实时写入磁盘

                    # 等待进程完成，带超时
                    process.wait(timeout=timeout)

                    # 更新状态
                    status = 'completed' if process.returncode == 0 else 'error'
                    with app.app_context():
                        update_crawler_status(run_id, status)

                except subprocess.TimeoutExpired:
                    # 超时处理
                    process.kill()
                    with open(log_path, 'a', encoding='utf-8') as log_file:
                        log_file.write(f"\n错误: 爬虫运行超时({timeout}秒)")
                    with app.app_context():
                        update_crawler_status(run_id, 'timeout')

                except Exception as e:
                    # 其他进程错误
                    with open(log_path, 'a', encoding='utf-8') as log_file:
                        log_file.write(f"\n进程错误: {str(e)}")
                    with app.app_context():
                        update_crawler_status(run_id, 'error')

        except Exception as e:
            # 记录错误
            with open(log_path, 'a', encoding='utf-8') as log_file:
                log_file.write(f"\n系统错误: {str(e)}")

            # 更新状态
            with app.app_context():
                update_crawler_status(run_id, 'error')

        finally:
            # 从活动爬虫中移除
            if run_id in self.active_crawlers:
                del self.active_crawlers[run_id]
    
    def add_scheduled_task(self, crawler_id, schedule_type, time_value):
        """添加定时任务"""
        crawler = self.get_crawler_by_id(crawler_id)
        if not crawler:
            return None
        
        task_id = str(uuid.uuid4())
        
        # 根据不同的调度类型设置任务
        if schedule_type == 'daily':
            # 每天执行，time_value格式为 HH:MM
            hour, minute = map(int, time_value.split(':'))
            job = self.scheduler.add_job(
                self.run_crawler,
                'cron',
                hour=hour,
                minute=minute,
                args=[crawler_id, 'scheduled', task_id]
            )
        elif schedule_type == 'interval':
            # 间隔执行，time_value为小时数
            hours = float(time_value)
            job = self.scheduler.add_job(
                self.run_crawler,
                'interval',
                hours=hours,
                args=[crawler_id, 'scheduled', task_id]
            )
        else:
            return None
        
        # 保存任务信息到内存
        self.scheduled_tasks[task_id] = {
            'job': job,
            'crawler_id': crawler_id,
            'crawler_name': crawler['name'],
            'schedule_type': schedule_type,
            'time_value': time_value
        }
        
        # 保存任务信息到数据库
        with self.app.app_context():
            db_add_scheduled_task(task_id, crawler_id, crawler['name'], schedule_type, time_value)
        
        return task_id
    
    def remove_scheduled_task(self, task_id):
        """移除定时任务"""
        if task_id in self.scheduled_tasks:
            # 从调度器中移除
            self.scheduled_tasks[task_id]['job'].remove()
            # 从任务列表中移除
            del self.scheduled_tasks[task_id]
            # 从数据库中移除
            with self.app.app_context():
                db_remove_scheduled_task(task_id)
            return True
        return False
    
    def get_scheduled_tasks(self):
        """获取所有定时任务"""
        # 从数据库获取任务列表
        with self.app.app_context():
            return db_get_scheduled_tasks()
        
    def _load_scheduled_tasks_from_db(self):
        """从数据库加载定时任务（在应用启动时调用）"""
        with self.app.app_context():
            tasks = db_get_scheduled_tasks()
            
        for task in tasks:
            task_id = task['id']
            crawler_id = task['crawler_id']
            schedule_type = task['schedule_type']
            time_value = task['time_value']
            
            # 根据不同的调度类型设置任务
            if schedule_type == 'daily':
                # 每天执行，time_value格式为 HH:MM
                hour, minute = map(int, time_value.split(':'))
                job = self.scheduler.add_job(
                    self.run_crawler,
                    'cron',
                    hour=hour,
                    minute=minute,
                    args=[crawler_id, 'scheduled', task_id]
                )
            elif schedule_type == 'interval':
                # 间隔执行，time_value为小时数
                hours = float(time_value)
                job = self.scheduler.add_job(
                    self.run_crawler,
                    'interval',
                    hours=hours,
                    args=[crawler_id, 'scheduled', task_id]
                )
            
            # 保存任务信息到内存
            self.scheduled_tasks[task_id] = {
                'job': job,
                'crawler_id': crawler_id,
                'crawler_name': task['crawler_name'],
                'schedule_type': schedule_type,
                'time_value': time_value
            }