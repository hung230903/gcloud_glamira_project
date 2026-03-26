import pandas as pd

def transform_summary_data(df):
    """
    Dùng để làm sạch dữ liệu TRƯỚC khi nạp vào Schema của PyArrow.
    Ép kiểu String/Int/Bool triệt để cho cả các RECORD lồng nhau.
    """
    # 0. Đổi tên trường để khớp với schema mới
    rename_rules = {
        'cat_id': 'category_id',
        'collect_id': 'collection_id'
    }
    for old_name, new_name in rename_rules.items():
        if old_name in df.columns:
            df = df.rename(columns={old_name: new_name})

    bool_cols = ['show_recommendation', 'recommendation', 'is_paypal']
    int_cols = [
        'recommendation_product_position', 'amount', 
        'product_id', 'viewing_product_id', 'recommendation_product_id',
        'category_id', 'collection_id', 'order_id'
    ]
    nested_cols = ['option', 'cart_products']

    def safe_bool(val):
        if pd.isna(val) or val == "" or val is None: return None
        s = str(val).lower().strip()
        if s in ['true', '1', 't', 'y', 'yes']: return True
        if s in ['false', '0', 'f', 'n', 'no']: return False
        return None

    # Hàm làm sạch sâu các Object và Array lồng nhau
    def clean_nested_list(val, is_cart_products=False):
        if not isinstance(val, list):
            return []
            
        cleaned_list = []
        for item in val:
            if not isinstance(item, dict):
                continue
            
            # Ép kiểu cho các field trong cart_products
            if is_cart_products:
                # product_id cast to INT64
                if 'product_id' in item:
                    try:
                        item['product_id'] = int(pd.to_numeric(item['product_id'], errors='coerce'))
                        if pd.isna(item['product_id']): item['product_id'] = None
                    except:
                        item['product_id'] = None
                
                # price, currency cast as STRING
                for k in ['price', 'currency']:
                    if k in item: item[k] = str(item[k]) if item[k] is not None else None
                
                # Ép kiểu Int hợp lệ cho amount
                if 'amount' in item:
                    try:
                        item['amount'] = int(pd.to_numeric(item['amount'], errors='coerce'))
                        if pd.isna(item['amount']): item['amount'] = 0
                    except:
                        item['amount'] = 0
                else:
                    item['amount'] = 0
                
                # Làm sạch sâu option lồng trong cart_products
                if 'option' not in item or not isinstance(item['option'], list):
                    item['option'] = []
                else:
                    for sub_opt in item['option']:
                        if isinstance(sub_opt, dict):
                            for f in ['option_id', 'option_label', 'value_id', 'value_label']:
                                if f in sub_opt: sub_opt[f] = str(sub_opt[f]) if sub_opt[f] is not None else None
            else:
                # Đổi tên trường lồng nhau để khớp schema
                if 'kollektion' in item: item['collection'] = item.pop('kollektion')
                if 'kollektion_id' in item: item['collection_id'] = item.pop('kollektion_id')

                # Ép kiểu String cho các field trong mảng 'option' ngoài cùng
                for field in [
                    'option_label', 'option_id', 'value_label', 'value_id', 
                    'quality', 'quality_label', 'alloy', 'diamond', 'shapediamond', 
                    'stone', 'pearlcolor', 'finish', 'price', 'collection'
                ]:
                    if field in item:
                        item[field] = str(item[field]) if item[field] is not None else None
                
                # Ép kiểu INT cho các ID trong mảng 'option'
                for int_f in ['category_id', 'collection_id']:
                    if int_f in item:
                        try:
                            item[int_f] = int(pd.to_numeric(item[int_f], errors='coerce'))
                            if pd.isna(item[int_f]): item[int_f] = None
                        except:
                            item[int_f] = None

            cleaned_list.append(item)
        return cleaned_list

    # Lặp qua tất cả các cột của DataFrame hiện tại
    for col in df.columns:
        if col in nested_cols:
            is_cart = (col == 'cart_products')
            df[col] = df[col].apply(lambda x: clean_nested_list(x, is_cart))
            continue
            
        if col in bool_cols:
            df[col] = df[col].apply(safe_bool).astype('boolean')
        elif col in int_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
        else:
            df[col] = df[col].apply(lambda x: str(x) if pd.notnull(x) else None)
            
    return df

def transform_ip2location_data(df):
    """
    Biến đổi dữ liệu IP2Location để khớp với kiểu FLOAT trên BigQuery.
    """
    if 'latitude' in df.columns:
        df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
    if 'longitude' in df.columns:
        df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
    return df
