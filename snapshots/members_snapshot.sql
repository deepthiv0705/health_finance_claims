{% snapshot members_snapshot %}
{{
    config(
        target_schema='snapshots',
        unique_key='member_id',
        strategy='check',
        check_cols=['state', 'age']
    )
}}

select * from {{ ref('stg_stg__members') }}

{% endsnapshot %}
