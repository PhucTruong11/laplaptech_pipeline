{{ config(
    materialized='incremental',
    unique_key='id',
    on_schema_change='sync_all_columns'
) }}

/*
  Silver Model: silver_user_event_tracking
  =========================================
  Mục đích:
    - Bóc tách (parsing) các thuộc tính quan trọng từ payload JSON thô (cột event_data và device).
    - Ép kiểu dữ liệu (timestamps, id) cho bảng sự kiện clickstream hơn 760.000 dòng.
    - Khử trùng lặp (Deduplication) khi nạp lại dữ liệu trong cửa sổ Lookback window.
    - Cấu hình Incremental Table để tối ưu hiệu năng: Chỉ xử lý dữ liệu mới phát sinh 
      thay vì phải parse lại hàng triệu bản ghi JSON ở mỗi lượt chạy pipeline.

  Cấu hình dbt Incremental:
    - materialized='incremental': Tạo bảng vật lý và tự động merge/insert dữ liệu mới.
    - unique_key='id': Khóa định danh duy nhất của mỗi sự kiện để cập nhật/loại trùng.
    - on_schema_change='sync_all_columns': Tự động cập nhật schema nếu có cột mới.
*/

WITH parsed AS (
    SELECT
        id,
        event_name,
        session_id,

        -- Chuyển đổi timestamp dạng epoch (số nguyên) sang kiểu TIMESTAMP chuẩn của PostgreSQL
        to_timestamp(event_local_timestamp)                  AS event_timestamp,
        to_timestamp(event_received_on_server_timestamp)     AS server_timestamp,

        -- Bóc tách dữ liệu từ JSON event_data:
        -- Dùng toán tử ->> để lấy giá trị dạng chuỗi (text)
        -- Kiểm tra Regex '^[0-9]+$' để lọc ra chỉ những device_id hợp lệ là số nguyên
        CASE 
            WHEN (event_data::json ->> 'device_id') ~ '^[0-9]+$' 
            THEN event_data::json ->> 'device_id'
            ELSE NULL 
        END                                                 AS device_id,
        event_data::json ->> 'page_name'                     AS page_name,
        event_data::json ->> 'keyword'                       AS search_keyword,
        event_data::json ->> 'sort_by'                       AS sort_by,
        event_data::json ->> 'sort_direction'                AS sort_direction,
        event_data::json ->> 'device_ids'                    AS device_ids_raw,

        -- Bóc tách dữ liệu thiết bị người dùng từ JSON device (User Agent metadata)
        device::json ->> 'os'                                AS user_os,
        device::json ->> 'os_version'                        AS user_os_version,
        device::json ->> 'browser'                           AS user_browser,
        device::json ->> 'device_type'                       AS user_device_type,

        -- Giữ lại trường JSON gốc để phục vụ truy vấn ad-hoc mở rộng khi cần
        event_data,
        device,

        -- Cơ chế Deduplication (Khử trùng lặp):
        -- Do ingestion script dùng Lookback Window (-1h) để chống sót dữ liệu trễ,
        -- có thể có bản ghi trùng lặp id được kéo lại. Ta dùng hàm ROW_NUMBER()
        -- đánh số theo thời gian server nhận mới nhất để chỉ lấy duy nhất 1 bản ghi chuẩn.
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY event_received_on_server_timestamp DESC
        ) AS row_num

    FROM {{ ref('bronze_user_event_tracking') }}
    WHERE event_name IS NOT NULL
      AND session_id IS NOT NULL

    {% if is_incremental() %}
    -- Lọc gia tăng: Khi chạy ở chế độ incremental, chỉ quét những sự kiện 
    -- có timestamp server nhận mới hơn mốc lớn nhất đang tồn tại trong bảng đích ({{ this }}).
    AND to_timestamp(event_received_on_server_timestamp) >= (
        SELECT COALESCE(MAX(server_timestamp) - INTERVAL '2 hour', '1970-01-01'::timestamp) 
        FROM {{ this }}
    )
    {% endif %}
)

-- Trích xuất các dòng đã deduplicate sạch sẽ (row_num = 1)
SELECT
    id,
    event_name,
    session_id,
    event_timestamp,
    server_timestamp,
    device_id,
    page_name,
    search_keyword,
    sort_by,
    sort_direction,
    device_ids_raw,
    user_os,
    user_os_version,
    user_browser,
    user_device_type,
    event_data,
    device
FROM parsed
WHERE row_num = 1