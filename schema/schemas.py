import pyarrow as pa

def get_summary_pyarrow_schema():
    """
    Định nghĩa Schema chuẩn của PyArrow để ép file Parquet có cấu trúc RECORD và REPEATED khớp với BigQuery.
    """
    option_fields = [
        pa.field("option_label", pa.string()),
        pa.field("option_id", pa.string()),
        pa.field("value_label", pa.string()),
        pa.field("value_id", pa.string()),
        pa.field("quality", pa.string()),
        pa.field("quality_label", pa.string()),
        pa.field("alloy", pa.string()),
        pa.field("diamond", pa.string()),
        pa.field("shapediamond", pa.string()),
        pa.field("stone", pa.string()),
        pa.field("pearlcolor", pa.string()),
        pa.field("finish", pa.string()),
        pa.field("price", pa.string()),
        pa.field("category_id", pa.int64()),
        pa.field("collection", pa.string()),
        pa.field("collection_id", pa.int64()),
    ]

    cart_option_fields = [
        pa.field("option_id", pa.string()),
        pa.field("option_label", pa.string()),
        pa.field("value_id", pa.string()),
        pa.field("value_label", pa.string()),
    ]

    cart_products_fields = [
        pa.field("product_id", pa.int64()),
        pa.field("price", pa.string()),
        pa.field("currency", pa.string()),
        pa.field("amount", pa.int64()),
        pa.field("option", pa.list_(pa.struct(cart_option_fields))),
    ]

    schema = pa.schema([
        ("time_stamp", pa.string()),
        ("ip", pa.string()),
        ("user_agent", pa.string()),
        ("resolution", pa.string()),
        ("user_id_db", pa.string()),
        ("device_id", pa.string()),
        ("api_version", pa.string()),
        ("store_id", pa.string()),
        ("local_time", pa.string()),
        ("show_recommendation", pa.bool_()),
        ("current_url", pa.string()),
        ("referrer_url", pa.string()),
        ("email_address", pa.string()),
        ("recommendation", pa.bool_()),
        ("utm_source", pa.string()),
        ("collection", pa.string()),
        ("product_id", pa.int64()),
        ("viewing_product_id", pa.int64()),
        ("category_id", pa.int64()),
        ("collection_id", pa.int64()),
        ("option", pa.list_(pa.struct(option_fields))),
        ("order_id", pa.int64()),
        ("recommendation_product_id", pa.int64()),
        ("recommendation_clicked_position", pa.string()),
        ("utm_medium", pa.string()),
        ("key_search", pa.string()),
        ("price", pa.string()),
        ("currency", pa.string()),
        ("recommendation_product_position", pa.int64()),
        ("is_paypal", pa.bool_()),
        ("cart_products", pa.list_(pa.struct(cart_products_fields))),
    ])
    return schema

def get_ip2location_pyarrow_schema():
    """
    Định nghĩa Schema chuẩn của PyArrow cho ip2location, khớp với BigQuery.
    """
    schema = pa.schema([
        ("ip", pa.string()),
        ("country", pa.string()),
        ("region", pa.string()),
        ("city", pa.string()),
        ("latitude", pa.float64()),
        ("longitude", pa.float64()),
    ])
    return schema

def get_product_info_pyarrow_schema():
    """
    Định nghĩa Schema chuẩn chi tiết nhất của PyArrow cho dữ liệu product_info.
    Bao gồm đầy đủ các trường lồng nhau để phục vụ Dimensional Modeling.
    Sử dụng Struct và List để tạo REPEATED RECORD trong BigQuery.
    """

    # Helper: Cấu trúc chung cho các thuộc tính chi tiết của Stone (carat, clarity, etc.)
    stone_attr_fields = [
        pa.field("default_label", pa.string()),
        pa.field("default_option_title", pa.string()),
        pa.field("label", pa.string()),
        pa.field("option_title", pa.string()),
        pa.field("value", pa.string()),
    ]
    stone_attr_struct = pa.struct(stone_attr_fields)

    # Helper: Cấu trúc data_stones, stone_gia, stone_quality dùng chung các thuộc tính
    stone_data_item_fields = [
        pa.field("carat", stone_attr_struct),
        pa.field("certificate", stone_attr_struct),
        pa.field("clarity", stone_attr_struct),
        pa.field("colour", stone_attr_struct),
        pa.field("cut", stone_attr_struct),
        pa.field("diameter", stone_attr_struct),
        pa.field("origin", stone_attr_struct),
        pa.field("origin_colour", stone_attr_struct),
        pa.field("qty", stone_attr_struct),
        pa.field("quality", stone_attr_struct),
        pa.field("shape", stone_attr_struct),
        pa.field("stone_name", stone_attr_struct),
        pa.field("stone_type", stone_attr_struct),
        pa.field("total_carat", stone_attr_struct),
        # Một số item có thêm id/label ở cấp ngoài
        pa.field("id", pa.string()),
        pa.field("label", pa.string()),
        pa.field("price", pa.string()),
    ]
    
    # Cấu trúc đặc thù cho stone_quality có thêm quality_origins lồng bên trong
    stone_quality_item_fields = stone_data_item_fields + [
        pa.field("quality_origins", pa.list_(pa.struct(stone_data_item_fields)))
    ]

    # 1. Media Image Struct
    image_item_fields = [
        pa.field("position", pa.int64()),
        pa.field("media_type", pa.string()),
        pa.field("label", pa.string()),
        pa.field("image_view", pa.string()),
        pa.field("is_default", pa.bool_()),
        pa.field("is_feature", pa.bool_()),
        pa.field("is_video", pa.bool_()),
        pa.field("area_view", pa.string()),
        pa.field("config", pa.string()), # JSON string if complex
        pa.field("watermark", pa.string()),
        pa.field("meta", pa.string()),
        pa.field("large_image_url", pa.string()),
        pa.field("medium_image_url", pa.string()),
        pa.field("medium_middle_image_url", pa.string()),
        pa.field("small_image_url", pa.string()),
        pa.field("sticky_image_url", pa.string()),
        pa.field("placeholder_alt", pa.string()),
    ]
    
    image_view_type_fields = [
        pa.field("type", pa.string()),
        pa.field("position", pa.int64()),
        pa.field("metadata", pa.string()),
    ]

    media_image_paths_fields = [
        pa.field("large_image_url", pa.string()),
        pa.field("medium_image_url", pa.string()),
        pa.field("medium_middle_image_url", pa.string()),
        pa.field("small_image_url", pa.string()),
        pa.field("sticky_image_url", pa.string()),
    ]

    media_image_fields = [
        pa.field("sku_image", pa.string()),
        pa.field("total_thumbs", pa.int64()),
        pa.field("default_position", pa.int64()),
        pa.field("image_load_type", pa.string()),
        pa.field("lcpMediaUrl", pa.string()),
        pa.field("images", pa.list_(pa.struct(image_item_fields))),
        pa.field("image_view_types", pa.list_(pa.struct(image_view_type_fields))),
        pa.field("paths", pa.struct(media_image_paths_fields)),
    ]

    # 2. Media Video Struct
    video_item_fields = [
        pa.field("id", pa.string()),
        pa.field("name", pa.string()),
        pa.field("label", pa.string()),
        pa.field("file_name", pa.string()),
        pa.field("url", pa.string()),
        pa.field("media_type", pa.string()),
        pa.field("hidden", pa.bool_()),
    ]
    media_video_fields = [
        pa.field("videos", pa.list_(pa.struct(video_item_fields)))
    ]

    # 3. Color, Custom, Stone Option Fields
    common_option_fields = [
        pa.field("option_type_id", pa.string()),
        pa.field("option_id", pa.string()),
        pa.field("sku", pa.string()),
        pa.field("title", pa.string()),
        pa.field("default_title", pa.string()),
        pa.field("store_title", pa.string()),
        pa.field("price", pa.string()),
        pa.field("price_type", pa.string()),
        pa.field("is_default", pa.bool_()),
    ]
    
    color_fields = common_option_fields + [
        pa.field("colour", pa.string()),
        pa.field("metal", pa.string()),
        pa.field("colour_label", pa.string()),
        pa.field("metal_label", pa.string()),
    ]
    
    stone_fields = common_option_fields + [
        pa.field("stone_group", pa.string()),
        pa.field("configure_quality", pa.string()),
        pa.field("default_quality", pa.string()),
        pa.field("data_stones", pa.list_(pa.struct(stone_data_item_fields))),
        pa.field("stone_gia", pa.list_(pa.struct(stone_data_item_fields))),
        pa.field("stone_quality", pa.list_(pa.struct(stone_quality_item_fields))),
    ]

    schema = pa.schema([
        ("product_id", pa.int64()), # ID dạng số trong JSON
        ("product_name", pa.string()),
        ("sku", pa.string()),
        ("attribute_set_id", pa.int64()),
        ("attribute_set", pa.string()),
        ("type_id", pa.string()),
        ("min_price", pa.string()),
        ("max_price", pa.string()),
        ("gold_weight", pa.string()),
        ("none_metal_weight", pa.float64()),
        ("fixed_silver_weight", pa.float64()),
        ("material_design", pa.string()),
        ("collection", pa.string()),
        ("collection_id", pa.int64()),
        ("product_type", pa.string()),
        ("product_type_value", pa.string()),
        ("category_id", pa.int64()),
        ("category_name", pa.string()),
        ("store_id", pa.string()),
        ("gender", pa.string()),
        
        # Nested Structures
        ("media_image", pa.struct(media_image_fields)),
        ("media_video", pa.struct(media_video_fields)),
        ("stone", pa.list_(pa.struct(stone_fields))),
        ("color", pa.list_(pa.struct(color_fields))),
        ("custom", pa.list_(pa.struct(common_option_fields))),
    ])

    return schema
