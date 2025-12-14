{% macro claim_value_bucket(amount) %}
    case
        when {{ amount }} >= 50000 then 'HIGH'
        else 'NORMAL'
    end
{% endmacro %}
