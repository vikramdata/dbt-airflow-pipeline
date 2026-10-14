{#
  Custom generic test: fails for any row where the column is negative.
  Usage in YAML:   tests: [positive_value]
#}
{% test positive_value(model, column_name) %}
select *
from {{ model }}
where {{ column_name }} < 0
{% endtest %}
