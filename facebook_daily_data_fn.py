# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np

# 選擇事件資訊(action_col)，並分成原版(value)跟各歸因版本(click_7d ...)
def json_get_data_fn(data,action_col):
    if isinstance(data, pd.DataFrame):
        # 先確認json欄位中是否有值
        # 再確認該事件有沒有值，沒值的話各欄位代0
        is_empyt = sum(data['action_type'] == action_col)
        if is_empyt == 0:
            df = pd.DataFrame({'action_type' :action_col, 'value' : [0], 'click_7d' : [0], 'view_1d' : [0]})
        else:
            df = data[data['action_type']==action_col]
    else:
        # 如果Json欄位中為NULL，各欄位代0
        df = pd.DataFrame({'action_type' :action_col, 'value' : [0], 'click_7d' : [0], 'view_1d' : [0]})
    # 調整欄位名稱
    df = df.rename({"7d_click" : "click_7d", "1d_view" : "view_1d"}, axis = 'columns')
    return(df)


# 選擇歸因數據，該欄位有數據就保持原數據，如果沒有數據都代0
# (可能會有有7d_click欄位、無1d_view欄位的狀況，所以需要此function)
def attribution_fn(df,df_attribution):
    if df_attribution not in df.columns:
        attribution = pd.Series(np.repeat(0, len(df)))
    else:
        attribution = df[df_attribution]
    return(attribution)


# 轉換json資料
def json_trans_fn(data):
    # output表中去除等等要轉換的conversions等欄位
    output = data.copy()
    for col in ['conversions', 'unique_actions', 'actions', 'action_values']:
        if col in data.columns:
            output = output.drop([col], axis = 1)
    
    # ===== 1. conversions (轉換數) =====
    if 'conversions' in data.columns:
        # 欄位內原本是包含dict的list，轉成DataFrame (若是nan則保持原樣)
        data['conversions'] = data['conversions'].apply(lambda x : pd.DataFrame(x) if isinstance(x, list) else x)
        
        # (1) app_custom_event 初版自定義事件 (=舊版的AEO_01)
        t = data.apply(lambda x : json_get_data_fn(data = x['conversions'], action_col = 'app_custom_event.four_hour'), axis=1)
        conversions_4hr = pd.concat([x for x in t]).reset_index()
        # (2) AEO 自定義事件
        t = data.apply(lambda x : json_get_data_fn(data = x['conversions'], action_col = 'app_custom_event.customEvent1'), axis=1)
        AEO_conversions_01 = pd.concat([x for x in t]).reset_index()
        t = data.apply(lambda x : json_get_data_fn(data = x['conversions'], action_col = 'app_custom_event.customEvent2'), axis=1)
        AEO_conversions_02 = pd.concat([x for x in t]).reset_index()
        t = data.apply(lambda x : json_get_data_fn(data = x['conversions'], action_col = 'app_custom_event.customEvent3'), axis=1)
        AEO_conversions_03 = pd.concat([x for x in t]).reset_index()
        # (3) registrate
        t = data.apply(lambda x : json_get_data_fn(data = x['conversions'], action_col = 'offsite_conversion.fb_pixel_custom.registrate'), axis=1)
        custom_registrate = pd.concat([x for x in t]).reset_index()
        
        output['conversions_7d_click_temp'] = attribution_fn(df = conversions_4hr , df_attribution = 'click_7d').fillna(0).astype(float)
        output['conversions_1d_view_temp'] = attribution_fn(df = conversions_4hr , df_attribution = 'view_1d').fillna(0).astype(float)
        output['AEO_01_7d_click_temp'] = attribution_fn(df = AEO_conversions_01 , df_attribution = 'click_7d').fillna(0).astype(float)
        output['AEO_01_1d_view_temp'] = attribution_fn(df = AEO_conversions_01 , df_attribution = 'view_1d').fillna(0).astype(float)
        output['AEO_02_7d_click'] = attribution_fn(df = AEO_conversions_02 , df_attribution = 'click_7d').fillna(0).astype(float)
        output['AEO_02_1d_view'] = attribution_fn(df = AEO_conversions_02 , df_attribution = 'view_1d').fillna(0).astype(float)
        output['AEO_03_7d_click'] = attribution_fn(df = AEO_conversions_03 , df_attribution = 'click_7d').fillna(0).astype(float)
        output['AEO_03_1d_view'] = attribution_fn(df = AEO_conversions_03 , df_attribution = 'view_1d').fillna(0).astype(float)
        output['registrate_7d_click'] = attribution_fn(df = custom_registrate , df_attribution = 'click_7d').fillna(0).astype(float)
        output['registrate_1d_view'] = attribution_fn(df = custom_registrate , df_attribution = 'view_1d').fillna(0).astype(float)
        
        # 特殊處理，AEO_01 = conversions + AEO_01
        output['AEO_01_7d_click'] = (output['conversions_7d_click_temp'] + output['AEO_01_7d_click_temp'])
        output['AEO_01_1d_view'] = (output['conversions_1d_view_temp'] + output['AEO_01_1d_view_temp'])
    else:
        # 無conversions欄位，數值全代0
        output['AEO_01_7d_click'] = 0
        output['AEO_01_1d_view'] = 0
        output['AEO_02_7d_click'] = 0
        output['AEO_02_1d_view'] = 0
        output['AEO_03_7d_click'] = 0
        output['AEO_03_1d_view'] = 0
        output['registrate_7d_click'] = 0
        output['registrate_1d_view'] = 0
        
    # ===== 2. unique_actions (不重複事件) =====
    ''' 
    # 2024/10/30 FB棄用
    if 'unique_actions' in data.columns:
        # 欄位內原本是包含dict的list，轉成DataFrame (若是nan則保持原樣)
        data['unique_actions'] = data['unique_actions'].apply(lambda x : pd.DataFrame(x) if isinstance(x, list) else x)
        
        # unique_purchase 不重複儲值人數
        t = data.apply(lambda x : json_get_data_fn(data = x['unique_actions'], action_col = 'app_custom_event.fb_mobile_purchase'), axis=1)
        unique_purchase = pd.concat([x for x in t]).reset_index()
        
        output['unique_purchase'] = attribution_fn(df = unique_purchase , df_attribution = 'value').fillna(0).astype(float)
        output['unique_purchase_7d_click'] = attribution_fn(df = unique_purchase , df_attribution = 'click_7d').fillna(0).astype(float)
        output['unique_purchase_1d_view'] = attribution_fn(df = unique_purchase , df_attribution = 'view_1d').fillna(0).astype(float)
        
    else:
        # 無unique_actions欄位，數值全代0
        output['unique_purchase'] = 0
        output['unique_purchase_7d_click'] = 0
        output['unique_purchase_1d_view'] = 0
    '''
        
    # ===== 3. batch 下載數 & lead ads & 粉絲團成果 =====
    if "actions" in data.columns:
        # 欄位內原本是包含dict的list，轉成DataFrame (若是nan則保持原樣)
        data['actions'] = data['actions'].apply(lambda x : pd.DataFrame(x) if isinstance(x, list) else x)
        
        # (1)mobile_app_install
        t = data.apply(lambda x : json_get_data_fn(data = x['actions'], action_col = 'mobile_app_install'), axis=1)
        mobile_app_install = pd.concat([x for x in t]).reset_index()
        # (2)onsite_conversion.lead_grouped
        t = data.apply(lambda x : json_get_data_fn(data = x['actions'], action_col = 'onsite_conversion.lead_grouped'), axis=1)
        leadgen = pd.concat([x for x in t]).reset_index()
        # (3)like
        t = data.apply(lambda x : json_get_data_fn(data = x['actions'], action_col = 'like'), axis=1)
        likes = pd.concat([x for x in t]).reset_index()
        # (4)link_click
        t = data.apply(lambda x : json_get_data_fn(data = x['actions'], action_col = 'link_click'), axis=1)
        link_click = pd.concat([x for x in t]).reset_index()
        
        output['app_install'] = attribution_fn(df = mobile_app_install , df_attribution = 'value').fillna(0).astype(float)
        output['app_install_7d_click'] = attribution_fn(df = mobile_app_install , df_attribution = 'click_7d').fillna(0).astype(float)
        output['app_install_1d_view'] = attribution_fn(df = mobile_app_install , df_attribution = 'view_1d').fillna(0).astype(float)
        output['leadgen'] = attribution_fn(df = leadgen , df_attribution = 'value').fillna(0).astype(float)
        output['leadgen_7d_click'] = attribution_fn(df = leadgen , df_attribution = 'click_7d').fillna(0).astype(float)
        output['leadgen_1d_view'] = attribution_fn(df = leadgen , df_attribution = 'view_1d').fillna(0).astype(float)
        output['likes'] = attribution_fn(df = likes , df_attribution = 'value').fillna(0).astype(float)
        output['likes_7d_click'] = attribution_fn(df = likes , df_attribution = 'click_7d').fillna(0).astype(float)
        output['likes_1d_view'] = attribution_fn(df = likes , df_attribution = 'view_1d').fillna(0).astype(float)
        output['link_click_7d_click'] = attribution_fn(df = link_click , df_attribution = 'click_7d').fillna(0).astype(float)
        output['link_click_1d_view'] = attribution_fn(df = link_click , df_attribution = 'view_1d').fillna(0).astype(float)
        
    else:
        # 無actions欄位，數值全代0
        output['app_install'] = 0
        output['app_install_7d_click'] = 0
        output['app_install_1d_view'] = 0
        output['leadgen'] = 0
        output['leadgen_7d_click'] = 0
        output['leadgen_1d_view'] = 0
        output['likes'] = 0
        output['likes_7d_click'] = 0
        output['likes_1d_view'] = 0
        output['link_click_7d_click'] = 0
        output['link_click_1d_view'] = 0
    
    # ===== 4. batch 儲值金額 =====
    if "action_values" in data.columns:
        # 欄位內原本是包含dict的list，轉成DataFrame (若是nan則保持原樣)
        data['action_values'] = data['action_values'].apply(lambda x : pd.DataFrame(x) if isinstance(x, list) else x)
        
        # fb_mobile_purchase 儲值金額
        t = data.apply(lambda x : json_get_data_fn(data = x['action_values'], action_col = 'app_custom_event.fb_mobile_purchase'), axis=1)
        fb_mobile_purchase = pd.concat([x for x in t]).reset_index()
        
        output['purchase'] = attribution_fn(df = fb_mobile_purchase , df_attribution = 'value').fillna(0).astype(float)
        output['purchase_7d_click'] = attribution_fn(df = fb_mobile_purchase , df_attribution = 'click_7d').fillna(0).astype(float)
        output['purchase_1d_view'] = attribution_fn(df = fb_mobile_purchase , df_attribution = 'view_1d').fillna(0).astype(float)
        
    else:
        # 無actions欄位，數值全代0
        output['purchase'] = 0
        output['purchase_7d_click'] = 0
        output['purchase_1d_view'] = 0
    
    return (output)