{% macro get_vendornames(vendorid) %}
    CASE 
        WHEN {{ vendorid }} = 1 THEN 'Creative Mobile Technologies'
        WHEN {{ vendorid }} = 2 THEN 'Verifone Inc'
        WHEN {{ vendorid }} = 4 THEN 'Unknown'
        
    END
{% endmacro %}