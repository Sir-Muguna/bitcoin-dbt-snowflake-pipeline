with whales as (

    select 
        output_address,
        sum(output_value) as total_sent,
        count(*) as tx_count
    from {{ ref('stg_btc_transactions') }}
    where output_value > 10
    group by output_address

)

select 
    w.output_address,
    round(w.total_sent, 3) as total_sent,
    w.tx_count,
    round({{ convert_to_usd('w.total_sent') }}, 3) as total_sent_usd
from whales w
order by w.total_sent desc
