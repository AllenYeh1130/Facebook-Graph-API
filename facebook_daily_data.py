# -*- coding: utf-8 -*-
project_name = 'facebook_daily_data'

### 載入套件
import json
import pandas as pd
import sys
import logging
import datetime as dt
import yagmail
import yaml
import requests
import sqlalchemy
import os
from facebook_config import get_ip_address, config_read
from facebook_daily_data_fn import json_trans_fn
parser_n = 1000 # 每次SQL寫入筆數

# 設定工作目錄為腳本所在目錄
os.chdir(os.path.dirname(os.path.abspath(__file__)))


#%%
### 設定檔
# 目前IP，用來判斷是在window還是linux環境
ip_address = get_ip_address()

# DB、email 設定檔
config = config_read(file_name = 'all_setting.json')

# FB 設定檔
f = open('facebook_setting.yml', 'r',encoding="utf-8")
facebook_daily_config_data = yaml.load(f, Loader=yaml.FullLoader)


#%%
### log設定
today = str(dt.date.today())
logpath = f"Log/{project_name}_{today}.log"

# 建立log資料夾
if not os.path.exists("Log"):
    os.makedirs("Log")
if not os.path.exists(logpath):
    with open(logpath, 'x') as file:  #使用 with 語句自動關閉檔案
        pass

if os.path.exists(logpath) == False:
    file = open(logpath, 'x')
    file.close()
logger = logging.getLogger(project_name)

# log檔-設定
logging.basicConfig(filename = logpath,
                    level = logging.INFO,
                    format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt = '%Y-%m-%d %H:%M:%S')


#%%
### 設定日期
try:
    arg = sys.argv
    if len(arg) > 1:
        run_mode = 'hour' if pd.isnull(arg[1]) else arg[1]        
        start_date = arg[2]
        end_date = arg[3]
        
    else:
        run_mode = 'hour'
        start_date = (dt.date.today() - dt.timedelta(days=2)).strftime('%Y-%m-%d')
        end_date = dt.date.today().strftime('%Y-%m-%d')
        
    # start_date = '2024-08-01'
    # end_date = '2024-08-02'
    
    execute_time = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    
    #%%    
    ### 連接資料庫
    USER = config['DB.MySQL.user']
    PASSWORD = config['DB.MySQL.pass']
    HOST = config['DB.MySQL.host']
    PORT = config['DB.MySQL.port']
    DATABASE = config['DB.MySQL.name']

    engine_stmt = 'mysql+pymysql://%s:%s@%s:%s/%s' % (USER,PASSWORD,HOST,PORT,DATABASE)
    engine = sqlalchemy.create_engine(engine_stmt, echo=False)
    
    
    #%%
    # ===== Work start =====
    logger.info(f"facebook_daily_data.R, 執行模式: {run_mode}, 起始日期: {start_date}, 結束日期: {end_date}")
    print(f"facebook_daily_data.R, 執行模式: {run_mode}, 起始日期: {start_date}, 結束日期: {end_date}")
    print("facebook_daily_data.R START")
    
    # 使用者帳號
    fb_user_id = facebook_daily_config_data['fb_user']
    # api版本
    fb_api = facebook_daily_config_data['fb_api']
    # token (有新account要在這新增變數)
    account_1_token = facebook_daily_config_data['access_token']['account_1']
    account_2_token = facebook_daily_config_data['access_token']['account_2']
    
    ### 取得campaign_list
    ### SQL輸入campaign_list
    # sql = f"SELECT app_no as account, date, campaign_id FROM fb_insight_app_status \
    #        WHERE date BETWEEN '{start_date}' and '{end_date}' AND code = 'campaign_id';"
    # campaign_list = pd.read_sql(sql, engine)
    ### 設定檔輸入campaign_list 
    campaign_list = pd.DataFrame(facebook_daily_config_data['account_campaign'])
    
    # 根據不同campaign確認有多少帳號要執行
    if len(campaign_list) > 0:
        run_account = campaign_list['account'].unique().tolist()
    
    if 'run_account' in locals():
        # 一次batch最大使用量(最多50)
        fb_max_request = facebook_daily_config_data['fb_max_request']
        # 紀錄成功的campaign_id資訊
        success_campaign_id = pd.DataFrame()
        # 紀錄失敗的campaign_id資訊
        fail_campaign_id = pd.DataFrame()
        # 紀錄失敗的資訊(寄信通知用)
        fail_information = pd.DataFrame()
        # campaign有錯重複查while條件
        fail_campaign_while = 0
        # while最多重複10次，初始設為0
        while_times = 0
        # 彙整取得campaign的所有資料
        merge_data = pd.DataFrame()
        
        # batch api設定
        fields = "campaign_id,adset_id,ad_id,impressions,reach,clicks,spend,actions,action_values,conversions"
        ad_level = "ad"
        # breakdowns = "['device_platform','publisher_platform']"，IOS14.5後不再提供此維度
        breakdowns = "[]"
        filterings = "[{'field':'ad.effective_status','operator':'IN','value':['ACTIVE','PAUSED','DELETED','PENDING_REVIEW','DISAPPROVED','PREAPPROVED','PENDING_BILLING_INFO','CAMPAIGN_PAUSED','ARCHIVED','ADSET_PAUSED','IN_PROCESS','WITH_ISSUES']},{'field':'impressions','operator':'GREATER_THAN','value':-1}]"
        time_range = "{'since':'%s','until':'%s'}" %(start_date, end_date)
        time_increment = 1 # 一天為資料單位
        limit_nrow = 1000 # 每頁資料限制筆數 (天數長可能會超過限制筆數並分頁)
        attribution_windows = "['7d_click','1d_view']"
        
        #%%
        ### i迴圈：跑各帳號
        for i in range(len(run_account)):
            # i = 0
            account = run_account[i]
            logger.info(f"正更新帳號: {account}")
            
            # 判斷要用哪個token
            if account == 'account_1' :
                access_token = account_1_token
            elif account == 'account_2' :
                access_token = account_2_token
            else:
                access_token = None
            
            # 使用者的廣告帳戶
            ad_campaign = campaign_list.query(f"account == '{account}'")['campaign_id'].unique()
            
            while (fail_campaign_while == 0) & (while_times < 10) : 
                # 第一次先跑全部campaign，如果有失敗campaign只跑失敗的
                if len(fail_campaign_id) == 0:
                    ad_campaign = ad_campaign
                else:
                    ad_campaign = fail_campaign_id['campaign_id'].unique()
                    
                # fail_campaign_id重設
                fail_campaign_id = pd.DataFrame()
                
                if len(ad_campaign) > 0:
                    ### ==== 要跑幾次batch，預設一個batch為50個campaign ====
                    batch_request_time = list(range(0, len(ad_campaign), fb_max_request))
                    
                    ### j迴圈：第j批batch
                    for j in range(len(batch_request_time)):
                        # j = 0
                        # *************Sys.sleep(10)
                        logger.info(f"第{j+1}次batch(上限50筆Campaign)要求資料")
                        print(f"第{j+1}次batch(上限50筆Campaign)要求資料")
                        
                        # 每次batch資料暫存位置(迴圈最後會合併至merge_data)
                        batch_data = pd.DataFrame()
                        
                        # batch_campaign_id，一次最多取50個campaign
                        start_ind = batch_request_time[j]
                        end_ind = min(batch_request_time[j] + fb_max_request, len(ad_campaign))
                        batch_campaign_id = ad_campaign[start_ind:end_ind]
                        
                        ### ==== 組合batch API Json格式 ====
                        relative_url = list(map(lambda x : f'{fb_api}/{x}/insights?fields={fields}&level={ad_level}&breakdowns={breakdowns}&filtering={filterings}&time_range={time_range}&time_increment={time_increment}&limit={limit_nrow}&action_attribution_windows={attribution_windows}&use_unified_attribution_setting=true', batch_campaign_id))
                        batch_body = list(map(lambda x : f'{{"method":"GET","relative_url":"{x}"}}', relative_url))
                        batch_body = '[' + ', '.join(batch_body) + ']'
                        
                        
                        # # url自己串，但relative_url的&要用urllib改成%26
                        # # 完整url為https://graph.facebook.com/me?batch=[...]&aceess_token=...
                        # import urllib
                        # url = "https://graph.facebook.com/me?batch="
                        # fb_graph_url = url+urllib.parse.quote(batch_body)+"&access_token="+access_token
                        # data=requests.post(fb_graph_url).json()
                        
                        ### ==== 執行batch API ====
                        try:
                            # 用params參數，?後的url會自動編碼
                            url = 'https://graph.facebook.com'
                            payload = {'batch': batch_body, 'access_token': access_token}
                            r = requests.post(url, params = payload, timeout = 180)
                        except Exception as e:
                            # ===== 紀錄執行異常 1. batch api =====
                            logger.info(f'[{execute_time}] "執行異常 1. batch api:" {batch_request_time[j]} {account}')
                            print(f'[{execute_time}] "執行異常 1. batch api:" {batch_request_time[j]} {account}')
                              
                            # timeout例外處理，紀錄失敗的campaign_id
                            fail_batch_campaign_id = pd.DataFrame({"app_no" : account, 
                                                                  "date" : end_date, 
                                                                  "campaign_id" : batch_campaign_id, 
                                                                  "code" : 'daily_data', 
                                                                  "status" : 0, 
                                                                  "update_time" : execute_time}, 
                                                                  index=[0])
                            
                            fail_campaign_id = fail_campaign_id.append(fail_batch_campaign_id)
                            
                            # 紀錄fail_information
                            fail_information = fail_information.append(
                                pd.DataFrame({ "error_message" : f"daily_data batch api error - error_message : {e}"}, index=[0]))
                            
                            
                        # 如果抓API時失敗就會有fail_batch_campaign_id，直接結束這次j迴圈
                        if 'fail_batch_campaign_id' in locals():
                            continue
                        
                        logger.info(f"拆解第 {j}批batch資料，共 {end_ind} 筆Campaign，正在處理 第 {start_ind}- {end_ind} 筆")
                        print(f"拆解第 {j+1}批batch資料，共 {end_ind} 筆Campaign，正在處理 第 {start_ind}- {end_ind} 筆")
                        
                        ### ==== 確認這次batch是否成功，並把資料撈出 ====
                        if r.status_code == 200:
                            # API list加入campaign_id資訊
                            batch_request_campaign_id = pd.DataFrame(r.json())
                            batch_request_campaign_id['campaign_id'] = batch_campaign_id
                            
                            ### k迴圈：第k個campaign
                            for k in range(len(batch_request_campaign_id)):
                                x = batch_request_campaign_id.iloc[k]
                                
                                # 最後輸出資料
                                output = pd.DataFrame()
                                # while停止設定
                                stop_while = 0
                                # 執行分頁次數
                                paging_cnt = 0
                                
                                while stop_while==0:
                                    if paging_cnt == 0:
                                        if x['code'] == 200:
                                            json_data = pd.DataFrame(json.loads(x['body'])['data'])
                                            
                                            if len(json_data) > 0:
                                                # 拆解json_data，把拆解結果合併到output
                                                temp_data = json_trans_fn(json_data)
                                                output = output.append(temp_data, ignore_index=True)
                                            
                                            ### 檢查是否有下個分頁
                                            json_body = json.loads(x['body'])
                                            if 'paging' in json_body.keys():
                                                if 'next' in json_body['paging'].keys():
                                                    # 有下個分頁的話，進入分頁處理
                                                    next_url = json.loads(x['body'])['paging']['next']
                                                    paging_cnt += 1
                                                else:
                                                    # 沒下一個分頁的話，結束while迴圈
                                                    stop_while = 1
                                            else:
                                                # 沒下一個分頁的話，結束while迴圈
                                                stop_while = 1
                                                
                                        else:
                                            # x['code'] != 200，紀錄失敗campaign_id
                                            fail_json_campaign_id = pd.DataFrame({"app_no" : account, 
                                                                                  "date" : end_date, 
                                                                                  "campaign_id" : x['campaign_id'], 
                                                                                  "code" : 'daily_data', 
                                                                                  "status" : 0, 
                                                                                  "update_time" : execute_time}, 
                                                                                 index=[0])
                                            fail_campaign_id = fail_campaign_id.append(fail_json_campaign_id)
                                            
                                            # 紀錄回傳錯誤
                                            fail_information = fail_information.append(pd.DataFrame({"error_message" : 
                                                                                                     "daily_insight error - campaign_id: {x['campaign_id']} error_message : {x['body']}"}, index=[0]))
                                            # 停止該campaign
                                            stop_while = 1
                                    else:
                                        # paging_cnt != 0 ，此時有多個分頁資料(因資料筆數超出limit_nrow)
                                        # ==== 分頁處理 ====
                                        logger.info(f"超出limit_nrow，讀取url接續資料，Campaign_id:{x['campaign_id']}")
                                        print(f"超出limit_nrow，讀取url接續資料，Campaign_id:{x['campaign_id']}")
                                        
                                        try:
                                            g = requests.get(next_url, timeout = 180)
                                        except Exception as e:
                                            # ===== 執行異常 2. batch paging =====
                                            logger.info("{exe_datetime} 執行異常 2. batch paging:{batch_request_time[j]} {account_no}, {dt.datetime.now()}")
                                            print("{exe_datetime} 執行異常 2. batch paging:{batch_request_time[j]} {account_no}, {dt.datetime.now()}")
                                            
                                            # 紀錄失敗campaign_id
                                            fail_json_campaign_id = pd.DataFrame({"app_no" : account, 
                                                                                  "date" : end_date, 
                                                                                  "campaign_id" : x['campaign_id'], 
                                                                                  "code" : 'daily_data', 
                                                                                  "status" : 0, 
                                                                                  "update_time" : execute_time}, 
                                                                                 index=[0])
                                            fail_campaign_id = fail_campaign_id.append(fail_json_campaign_id)
                                            
                                            # 紀錄回傳錯誤
                                            fail_information = fail_information.append(pd.DataFrame({"error_message" : 
                                                                                                     f"daily_insight error - error_message : {e}"}, index=[0]))
                                            # 停止該campaign
                                            stop_while = 1
                                        
                                        json_paging = pd.DataFrame(g.json()['data'])
                                        
                                        if len(json_paging) > 0:
                                            temp_data = json_trans_fn(json_paging)
                                            output = output.append(temp_data, ignore_index=True)
                                        
                                        ### 檢查是否還有下一頁
                                        if_next_url = g.json()['paging']
                                        if 'next' in if_next_url.keys():
                                            # 有下個分頁的話，進入分頁處理
                                            next_url = g.json()['paging']['next']
                                            paging_cnt += 1
                                        else:
                                            # 沒下一個分頁的話，結束while迴圈
                                            stop_while = 1
                                
                                    # while迴圈的最後，將output合併進batch_data
                                    batch_data = batch_data.append(output, ignore_index=True)
                                
                        else:
                            # 整個batch接資料失敗，紀錄fail_campaign_id
                            # ===== 執行異常 3. web_status =====
                            logger.info(f"{execute_time} 執行異常 3. web_status: {batch_request_time[j]} {account}, {dt.datetime.now()}")
                            print(f"{execute_time} 執行異常 3. web_status: {batch_request_time[j]} {account}, {dt.datetime.now()}")
                            
                            # 紀錄失敗campaign_id
                            fail_json_campaign_id = pd.DataFrame({"app_no" : account, 
                                                                  "date" : end_date, 
                                                                  "campaign_id" : batch_campaign_id, 
                                                                  "code" : 'daily_data', 
                                                                  "status" : 0, 
                                                                  "update_time" : execute_time}, 
                                                                 index=[0])
                            fail_campaign_id = fail_campaign_id.append(fail_json_campaign_id)
                            
                            # 紀錄回傳錯誤
                            batch_request_content = pd.DataFrame(r.json())
                            error_code = batch_request_content['error']['code']
                            error_message = batch_request_content['error']['message']
                            fail_information = fail_information.append(pd.DataFrame({"error_message" : 
                                                                                     f"daily_insight error - web_status : {r.status_code}  error_code : {error_code}  error_message : {error_message}"}, index=[0]))
            
                        # ==== 紀錄成功的campaign資料，加總每次batch的結果 ====
                        if len(batch_data) > 0:
                            # 紀錄有成功的campaign_id
                            temp_success_campaign_id = pd.DataFrame({"app_no" : account, 
                                                                     "date" : end_date, 
                                                                     "campaign_id" : batch_data['campaign_id'].unique(), 
                                                                     "code" : "daily_data", 
                                                                     "status" : 1, 
                                                                     "update_time" : execute_time})
                            success_campaign_id = success_campaign_id.append(temp_success_campaign_id)
            
                            # 調整欄位，沒有的欄位要補
                            if 'spend' not in batch_data.columns:
                              batch_data['spend'] = 0                        
                            if 'impressions' not in batch_data.columns:
                              batch_data['impressions'] = 0 
                            if 'reach' not in batch_data.columns:
                              batch_data['reach'] = 0 
                            if 'clicks' not in batch_data.columns:
                              batch_data['clicks'] = 0 
                            
                            batch_data['date'] = batch_data['date_stop']
                            batch_data = batch_data.drop(columns = ['date_start', 'date_stop'])
                            
                            batch_data = batch_data.query("impressions != '0' | reach != '0' | clicks != '0' | spend != '0' | app_install != 0 | leadgen != 0 | likes != 0 | purchase != 0")
                            
                            # 合併到merge_data
                            merge_data = merge_data.append(batch_data)
                            
                            
                    # ==== 有失敗的campaign資料時重跑while迴圈 ====
                    # 先排除不該出現在fail_campaign_id的campaign
                    if len(fail_campaign_id) > 0:
                        # (1)如果失敗的campaign在這次迴圈有成功即排除
                        if len(success_campaign_id) > 0:
                            fail_campaign_id = pd.merge(fail_campaign_id, success_campaign_id, on='campaign_id', how='outer', indicator=True)
                            fail_campaign_id = fail_campaign_id.query("_merge == 'left_only'").drop_duplicates()
                        else:
                            fail_campaign_id = fail_campaign_id.drop_duplicates()
                            
                        # (2)batch_request_campaign_id中沒撈到資料的campaign也排除
                        #    (避免部分campaign在campaign層有資料，但ad層無資料導致跳錯)
                        check_data_campaign = pd.DataFrame({"campaign_id" : batch_request_campaign_id['campaign_id'], 
                                                          "check" : 'V'})
                        
                        fail_campaign_id = pd.merge(fail_campaign_id, check_data_campaign, on='campaign_id', how='left')
                        fail_campaign_id = fail_campaign_id.query("check == 'V'").drop(columns = ['check'])
                    
                    if len(fail_campaign_id) > 0:
                        # fail_campaign_id有資料，繼續while迴圈，並記錄次數
                        fail_campaign_while = 0
                        while_times = while_times + 1
                        # 重製失敗訊息
                        fail_information = pd.DataFrame()
                        logger.info(f"facebook_daily_data 有{len(fail_campaign_id)}個失敗的Campaign，第{while_times}次重新要求資料")
                        print(f"facebook_daily_data 有{len(fail_campaign_id)}個失敗的Campaign，第{while_times}次重新要求資料")
                    else:
                        # fail_campaign_id無資料，停止while迴圈
                        fail_campaign_while = 1
                        logger.info("facebook_daily_data 沒有失敗的Campaign")
                        print("facebook_daily_data 沒有失敗的Campaign")
                        
                else:
                    # ad_campaign中沒有campaign資料，停止while迴圈
                    fail_campaign_while = 1
                    print("沒有Campaign 資訊")
            
            # 如果10次迴圈還有失敗campaign就寄信
            if (while_times == 10) & (len(fail_campaign_id) > 0):            
                # 寄信通知
                receive = config['mail.to'] # 收信者
                sub = f"facebook_daily_data 主程式無法完整更新 - {execute_time}" # 信件主旨
                content = f"警告訊息：\nfacebook_daily_data 更新時間: {start_date} - {end_date}\n尚有 {len(fail_campaign_id)} 筆Campaign未更新\
                    \n資料未全部更新完成，請務必注意資料有無缺失：\n {fail_campaign_id.to_html()}" # 信件內容
                
                yag = yagmail.SMTP(user = config['mail.from'], password = config['mail.from_password']) # 寄件者
                yag.send(to=receive, subject=sub, 
                         contents= content)
                
        # ===== API資料整合 =====
        if len(merge_data) > 0:
            # 更改欄位型態
            merge_data['impressions'] = merge_data['impressions'].astype(float)
            merge_data['reach'] = merge_data['reach'].astype(float)
            merge_data['clicks'] = merge_data['clicks'].astype(float)
            merge_data['spend'] = merge_data['spend'].astype(float)
            
            logger.info("API資料整合")
            print("API資料整合")
            
            ### 與mobile_game_fb_campaign join，取得channel、group_id
            # 取得手遊group_id, channel
            sql = "SELECT group_id, channel, campaign_id, adset_id, ad_id FROM mobile_game_fb_campaign;"
            fb_mobile_data = pd.read_sql(sql, engine)
            
            merge_data = pd.merge(merge_data, fb_mobile_data, how='left')
            merge_data = merge_data.groupby(['group_id' , 'channel', 'campaign_id', 'adset_id', 'ad_id', 'date'])\
                [['impressions', 'reach', 'clicks', 'spend', 'app_install', 'app_install_7d_click', 
                 'app_install_1d_view', 'purchase', 
                 'purchase_7d_click', 'purchase_1d_view', 'likes', 'likes_7d_click', 'likes_1d_view', 
                 'AEO_01_7d_click', 'AEO_01_1d_view', 'AEO_02_7d_click', 'AEO_02_1d_view', 
                 'AEO_03_7d_click', 'AEO_03_1d_view', 'leadgen', 'leadgen_7d_click', 'leadgen_1d_view', 
                 'link_click_7d_click', 'link_click_1d_view', 'registrate_7d_click', 'registrate_1d_view']]\
                .sum().reset_index()
            
        #%%
        ### 資料寫入
        ### 寫入 mobile_game_fb_daily_data
        # logger.info("寫入 mobile_game_fb_daily_data")
        # print("寫入 mobile_game_fb_daily_data")
        
        # if len(merge_data) > 0:
        #     # DUPLICATE_KEY_UPDATE_SQL
        #     DUPLICATE_KEY_UPDATE_str = ", ".join(["%s = VALUES(%s)" %(i, i) for i in merge_data.columns])
        #     DUPLICATE_KEY_UPDATE_SQL = " ON DUPLICATE KEY UPDATE " + DUPLICATE_KEY_UPDATE_str + ";"
        #     # columns
        #     cols = ", ".join(merge_data.columns)
            
        #     # insert_values
        #     for i in range(0, merge_data.shape[0], parser_n):
        #         start_ind = i            #    0, 1000, 2000
        #         end_ind = i + parser_n   # 1000, 2000, 2180
        #         if end_ind > merge_data.shape[0]:
        #             end_ind = merge_data.shape[0]
            
        #         temp_list = []
        #         for i, row in merge_data[start_ind:end_ind].iterrows():
        #             # 日期轉str
        #             row.date = str(row.date)
        #             temp_list.append(str(tuple(row)))
        #         insert_values = ', '.join(temp_list)
                
        #         sql = "INSERT mobile_game_fb_daily_data (" + cols + ") VALUES " + insert_values + DUPLICATE_KEY_UPDATE_SQL
        #         engine.execute(sql)
            
        #     logger.info("DB寫入完成")
        #     print("DB寫入完成")
        ### 匯出csv檔案
        logger.info("匯出成CSV檔案")
        print("匯出成CSV檔案")
        merge_data.to_csv('facebook_campaign_insight.csv', index=False, encoding='utf-8')
        
        # ===== 更新成功與失敗的campaign_id狀態 =====
        ### 成功campaign_id寫入fb_insight_app_status
        if len(success_campaign_id) > 0:
            # columns
            cols = ", ".join(success_campaign_id.columns)
            
            temp_list = []
            for i, row in success_campaign_id.iterrows():
                # 日期轉str
                row.date = str(row.date)
                temp_list.append(str(tuple(row)))
            insert_values = ', '.join(temp_list)
            
            sql = "INSERT fb_insight_app_status (" + cols + ") VALUES " + insert_values + " ON DUPLICATE KEY UPDATE status = VALUES(status), update_time = VALUES(update_time);"
            engine.execute(sql)
            
        ### 失敗campaign_id寫入fb_insight_app_status
        if len(fail_campaign_id) > 0:
            # columns
            cols = ", ".join(fail_campaign_id.columns)
            
            temp_list = []
            for i, row in fail_campaign_id.iterrows():
                # 日期轉str
                row.date = str(row.date)
                temp_list.append(str(tuple(row)))
            insert_values = ', '.join(temp_list)
            
            sql = "INSERT fb_insight_app_status (" + cols + ") VALUES " + insert_values + " ON DUPLICATE KEY UPDATE status = VALUES(status), update_time = VALUES(update_time);"
            engine.execute(sql)
            
        ### 寄信通知fail_information
        if len(fail_information) > 0:
            # 寄信通知
            receive = config['mail.to'] # 收信者
            sub = f"facebook_daily_data API錯誤訊息 - {execute_time}" # 信件主旨
            content = f"錯誤訊息：\n FB API執行失敗\n資料未全部更新完成，請務必注意資料有無缺失：\n {fail_information.to_html()}" # 信件內容
            
            yag = yagmail.SMTP(user = config['mail.from'], password = config['mail.from_password']) # 寄件者
            yag.send(to=receive, subject=sub, 
                     contents= content)
            
        ### 將程式結束時間寫入system_setting
        sql = f"INSERT system_setting (category, name, value) VALUES ('FB_API_data_update', 'facebook_api_daily_data', '{execute_time}') ON DUPLICATE KEY UPDATE value=VALUES(value);"
        engine.execute(sql)
    
    else:
        logger.info(f"{execute_time} no data for facebook_daily_data： {end_date}")
        print(f"{execute_time} no data for facebook_daily_data： {end_date}") 
#%%
### 錯誤處理
except Exception as e:
    logger.info(f"{execute_time} 執行異常 4. 非預期錯誤")
    print(f"{execute_time} 執行異常 4. 非預期錯誤")
    
    # campaign_list中的campaign都當成錯誤，寫入fb_insight_app_status
    if 'campaign_list' in locals():
        if len(campaign_list) > 0:
            fail_info = campaign_list.copy()
            fail_info['code'] = 'daily_data'
            fail_info['status'] = 0
            fail_info['update_time'] = execute_time
            
            # columns
            cols = ", ".join(fail_info.columns)
            
            temp_list = []
            for i, row in fail_info.iterrows():
                # 日期轉str
                row.date = str(row.date)
                temp_list.append(str(tuple(row)))
            insert_values = ', '.join(temp_list)
            
            sql = "INSERT fb_insight_app_status (" + cols + ") VALUES " + insert_values + " ON DUPLICATE KEY UPDATE status = VALUES(status), update_time = VALUES(update_time);"
            engine.execute(sql)
    
    import traceback
    # 產生完整的錯誤訊息
    error_class = e.__class__.__name__ #取得錯誤類型
    detail = e.args[0] #取得詳細內容
    tb = sys.exc_info()[2] #取得Call Stack
    lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
    fileName = lastCallStack[0] #取得發生的檔案名稱
    lineNum = lastCallStack[1] #取得發生的行號
    # funcName = lastCallStack[2] #取得發生的函數名稱
    errMsg = f"File \"{fileName}\", line {lineNum} \n錯誤訊息：\n [{error_class}] {detail}"
    
    # 開始寄信
    receive = config['mail.to'] # 收信者
    sub = f"facebook_daily_data.py  主程式錯誤訊息，時間:{execute_time}" # 信件主旨
    content = f"錯誤訊息：\n 非預期程式執行失敗\n {errMsg}\n資料未全部更新完成，請務必注意資料有無缺失" # 信件內容
    
    yag = yagmail.SMTP(user = config['mail.from'], password = config['mail.from_password']) # 寄件者
    yag.send(to=receive, subject=sub, contents= content)