{% macro get_recent_transactions_window(hours=24) %}
  timestamp >= CURRENT_TIMESTAMP - INTERVAL '{{ hours }} hours'
{% endmacro %}