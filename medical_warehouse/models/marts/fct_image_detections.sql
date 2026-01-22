with yolo as (

    select
        message_id,
        channel_name,
        detected_object,
        confidence_score,
        image_category
    from {{ source('raw', 'yolo_detections') }}

),

joined as (

    select
        f.message_id,
        c.channel_key,
        d.date_key,
        y.detected_object,
        y.confidence_score,
        y.image_category
    from yolo y
    join {{ ref('fct_messages') }} f on f.message_id = y.message_id
    join {{ ref('dim_channels') }} c on c.channel_name = y.channel_name
    join {{ ref('dim_dates') }} d on d.full_date = f.message_date

)

select * from joined
