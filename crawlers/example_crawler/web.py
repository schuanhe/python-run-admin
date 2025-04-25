from flask import Blueprint, render_template, jsonify, request
import os
import sqlite3
import json


# 创建蓝图
def create_blueprint(crawler_id, crawler_path):
    """
    为爬虫创建蓝图
    
    Args:
        crawler_id: 爬虫ID
        crawler_path: 爬虫路径
    """
    # 读取配置文件
    config_path = os.path.join(crawler_path, 'config.json')
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)

    # 获取数据库路径

    # 创建蓝图
    blueprint = Blueprint(
        f'crawler_{crawler_id}',
        __name__,
        template_folder=os.path.join(crawler_path, 'templates'),
        static_folder=os.path.join(crawler_path, 'static'),
        url_prefix=f'/crawler/{crawler_id}'
    )

    # 路由：爬虫Web界面首页
    @blueprint.route('/')
    def index():
        return render_template('crawler_data.html',
                               crawler_name=config.get('name', crawler_id),
                               crawler_id=crawler_id)

    # 路由：获取爬虫数据
    @blueprint.route('/data')
    def get_data():
        conn = sqlite3.connect(os.path.join(crawler_path, "crawler_data.db"))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM crawler_data ORDER BY created_at DESC")
        rows = cursor.fetchall()

        data = [dict(row) for row in rows]
        conn.close()

        return jsonify(data)

    # 路由：添加爬虫数据（用于测试）
    @blueprint.route('/add', methods=['POST'])
    def add_data():
        title = request.form.get('title')
        url = request.form.get('url')
        content = request.form.get('content')

        if not all([title, url]):
            return jsonify({'status': 'error', 'message': '标题和URL不能为空'}), 400

        conn = sqlite3.connect(os.path.join(crawler_path, "crawler_data.db"))
        cursor = conn.cursor()

        cursor.execute(
            "INSERT INTO crawler_data (title, url, content) VALUES (?, ?, ?)",
            (title, url, content)
        )

        conn.commit()
        conn.close()

        return jsonify({'status': 'success'})

    return blueprint
