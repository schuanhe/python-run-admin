import time
import random
import logging
import sys

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)

def main():
    logging.info("示例爬虫开始运行")
    print("print示例输出")
    
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