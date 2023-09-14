{{ config(
    indexes = [{'columns':['id'],'type':'btree'}],
    materialized='table',
    unique_key='id',
    schema="production"
) }}


SELECT
    tickets._airbyte_unique_key as id,
    tickets.created_at,
    properties.subject,
    properties.hs_ticket_priority as priotity,
    properties.content
FROM
    {{ ref('tickets') }} AS tickets
INNER JOIN
    {{ ref('tickets_properties') }} AS properties
ON
    tickets._airbyte_tickets_hashid = properties._airbyte_tickets_hashid
WHERE tickets.is_deleted = false

