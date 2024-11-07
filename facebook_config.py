# -*- coding: utf-8 -*-
import sys
import os
import json
import socket
import pandas as pd

# ===== config =====
def config_read(file_name):
    # Read config file
    if sys.platform.startswith('linux'):
        file_path = r'/root/python_project/python_config'  # linux文件路径
    elif sys.platform.startswith('win'):
        file_path = ''
    else:
        print("無法判斷程式執行的作業系統")

    file_path = os.path.join(file_path, file_name) #完整設定檔路徑
    #讀入json
    with open(file_path, 'r') as file:
        config = json.load(file)
    
    config = pd.json_normalize(config).stack().reset_index(level=0, drop=True) #刪除多的一層索引
    config = config.to_dict()
    return config

# ===== 判斷ip =====
# 獲得目前主機IP，要判斷是正式機還是測試機
def get_ip_address():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    ip_address = s.getsockname()[0]
    s.close()
    return ip_address
