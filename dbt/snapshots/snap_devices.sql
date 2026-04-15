{% snapshot snap_devices %}

{{
    config(
        unique_key='device_id',
        strategy='check',
        check_cols=['name', 'category', 'product_name', 'is_online'],
    )
}}

SELECT * FROM {{ ref('stg_devices_latest') }}

{% endsnapshot %}
