{# Human-friendly label for an onboarding step. #}
{% macro step_label(step) -%}
    {{ {"profile_photo": "Profile photo", "follow_5_accounts": "Follow 5 accounts",
        "first_post": "First post", "enable_notifications": "Enable notifications"}[step] }}
{%- endmacro %}
