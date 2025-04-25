from flask import Flask, render_template, request, jsonify, redirect, url_for
import os
import sqlite3
import json
import datetime
import time
import threading
import logging
import importlib.util
from database.models import init_db, get_db, close_db, add_crawler_run, update_crawler_status, get_crawler_runs, get_active_crawlers, get_crawler_by_id
from crawler_manager import CrawlerManager

app = Flask(__name__, template_folder='web/templates', static_folder='web/static')
app.config['DATABASE'] = os.path.join(app.instance_path, 'crawler.sqlite')

# 确保实例文件夹存在
try:
    os.makedirs(app.instance_path)
except OSError:
    pass

# 初始化数据库
with app.app_context():
    init_db()

# 初始化爬虫管理器
crawler_manager = CrawlerManager(app)

# 路由：首页
@app.route('/')
def index():
    return render_template('index.html')

# 路由：爬虫列表
@app.route('/crawlers')
def list_crawlers():
    # 获取爬虫列表，并添加web_support信息
    crawlers = []
    for crawler in crawler_manager.get_all_crawlers():
        crawler_info = crawler_manager.get_crawler_by_id(crawler['id'])
        if crawler_info:
            crawlers.append(crawler_info)
    
    active_crawlers = get_active_crawlers()
    active_ids = [c['id'] for c in active_crawlers]
    
    return render_template('crawlers.html', 
                           crawlers=crawlers, 
                           active_ids=active_ids)

# 路由：启动爬虫
@app.route('/crawlers/run/<crawler_id>', methods=['POST'])
def run_crawler(crawler_id):
    crawler = crawler_manager.get_crawler_by_id(crawler_id)
    if not crawler:
        return jsonify({'status': 'error', 'message': '爬虫不存在'}), 404
    
    # 启动爬虫（手动运行）
    run_id = crawler_manager.run_crawler(crawler_id, run_type='manual')
    
    return jsonify({'status': 'success', 'run_id': run_id})

# 路由：获取爬虫状态
@app.route('/crawlers/status')
def get_crawlers_status():
    active_crawlers = get_active_crawlers()
    return jsonify(active_crawlers)

# 路由：查看爬虫日志
@app.route('/logs/<run_id>')
def view_log(run_id):
    crawler_run = get_crawler_by_id(run_id)
    if not crawler_run:
        return jsonify({'status': 'error', 'message': '运行记录不存在'}), 404
    
    log_path = crawler_run['log_path']
    if not os.path.exists(log_path):
        return jsonify({'status': 'error', 'message': '日志文件不存在'}), 404
    
    with open(log_path, 'r', encoding='utf-8') as f:
        log_content = f.read()
    
    return render_template('log_viewer.html', 
                           crawler_name=crawler_run['crawler_name'],
                           start_time=crawler_run['start_time'],
                           log_content=log_content)

# 路由：获取日志内容（用于动态加载）
@app.route('/logs/content/<run_id>')
def get_log_content(run_id):
    crawler_run = get_crawler_by_id(run_id)
    if not crawler_run:
        return jsonify({'status': 'error', 'message': '运行记录不存在'}), 404
    
    log_path = crawler_run['log_path']
    if not os.path.exists(log_path):
        return jsonify({'status': 'error', 'message': '日志文件不存在'}), 404
    
    with open(log_path, 'r', encoding='utf-8') as f:
        log_content = f.read()
    
    return jsonify({'content': log_content})

# 路由：爬虫历史记录
@app.route('/history')
def crawler_history():
    runs = get_crawler_runs()
    # 增加运行类型和定时任务ID信息
    for run in runs:
        if 'run_type' not in run:
            run['run_type'] = 'manual'
        if 'schedule_id' not in run:
            run['schedule_id'] = None
    return render_template('history.html', runs=runs)

# 路由：定时任务管理页面
@app.route('/schedules')
def schedules():
    crawlers = crawler_manager.get_all_crawlers()
    scheduled_tasks = crawler_manager.get_scheduled_tasks()
    return render_template('schedules.html', 
                           crawlers=crawlers,
                           scheduled_tasks=scheduled_tasks)

# 路由：添加定时任务
@app.route('/schedules/add', methods=['POST'])
def add_schedule():
    crawler_id = request.form.get('crawler_id')
    schedule_type = request.form.get('schedule_type')
    time_value = request.form.get('time_value')
    
    if not all([crawler_id, schedule_type, time_value]):
        return jsonify({'status': 'error', 'message': '参数不完整'}), 400
    
    # 添加定时任务（会同时保存到内存和数据库）
    task_id = crawler_manager.add_scheduled_task(crawler_id, schedule_type, time_value)
    if not task_id:
        return jsonify({'status': 'error', 'message': '添加定时任务失败'}), 500
        
    return jsonify({'status': 'success', 'task_id': task_id})

# 路由：删除定时任务
@app.route('/schedules/delete/<task_id>', methods=['POST'])
def delete_schedule(task_id):
    success = crawler_manager.remove_scheduled_task(task_id)
    if success:
        return jsonify({'status': 'success'})
    else:
        return jsonify({'status': 'error', 'message': '任务不存在'}), 404

# 注册爬虫Web界面
def register_crawler_web_interfaces():
    """注册所有支持Web界面的爬虫"""
    crawlers = crawler_manager.get_all_crawlers()
    for crawler in crawlers:
        crawler_id = crawler['id']
        crawler_info = crawler_manager.get_crawler_by_id(crawler_id)
        
        if crawler_info and crawler_info.get('web_support', False):
            crawler_path = os.path.join(crawler_manager.crawlers_dir, crawler_id)
            web_module_path = os.path.join(crawler_path, 'web.py')
            
            if os.path.exists(web_module_path):
                try:
                    # 动态导入web.py模块
                    spec = importlib.util.spec_from_file_location(f"crawler_{crawler_id}_web", web_module_path)
                    web_module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(web_module)
                    
                    # 创建并注册蓝图
                    if hasattr(web_module, 'create_blueprint'):
                        blueprint = web_module.create_blueprint(crawler_id, crawler_path)
                        app.register_blueprint(blueprint)
                        logging.info(f"已注册爬虫Web界面: {crawler_id}")
                except Exception as e:
                    logging.error(f"注册爬虫Web界面失败: {crawler_id}, 错误: {str(e)}")

# 路由：访问爬虫Web界面
@app.route('/crawler_web/<crawler_id>')
def crawler_web(crawler_id):
    crawler = crawler_manager.get_crawler_by_id(crawler_id)
    if not crawler:
        return jsonify({'status': 'error', 'message': '爬虫不存在'}), 404
    
    if not crawler.get('web_support', False):
        return jsonify({'status': 'error', 'message': '该爬虫不支持Web界面'}), 404
    
    # 重定向到爬虫的Web界面
    return redirect(f"/crawler/{crawler_id}/")

# 注册爬虫Web界面
register_crawler_web_interfaces()

if __name__ == '__main__':
    app.run(debug=True)