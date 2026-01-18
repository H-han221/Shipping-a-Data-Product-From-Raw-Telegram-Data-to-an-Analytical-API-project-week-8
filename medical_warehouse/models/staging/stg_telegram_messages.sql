with raw as (
    select * from raw.telegram_messages
)

select
    message_id,
    lower(channel_name) as channel_name,
    message_date::timestamp as message_date,
    message_text,
    views::int as view_count,
    forwards::int as forward_count,
    has_media,
    image_path,
    length(message_text) as message_length,
    case when has_media then true else false end as has_image
from raw
where message_text is not null
