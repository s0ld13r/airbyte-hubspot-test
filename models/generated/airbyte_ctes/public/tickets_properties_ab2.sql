{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    schema = "_airbyte_public",
    tags = [ "nested-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('tickets_properties_ab1') }}
select
    _airbyte_tickets_hashid,
    cast({{ empty_string_to_null('closed_date') }} as {{ type_timestamp_with_timezone() }}) as closed_date,
    cast(hs_all_accessible_team_ids as {{ dbt_utils.type_string() }}) as hs_all_accessible_team_ids,
    cast(hs_all_conversation_mentions as {{ dbt_utils.type_string() }}) as hs_all_conversation_mentions,
    cast(hs_user_ids_of_all_notification_unfollowers as {{ dbt_utils.type_string() }}) as hs_user_ids_of_all_notification_unfollowers,
    cast(subject as {{ dbt_utils.type_string() }}) as subject,
    cast({{ empty_string_to_null('createdate') }} as {{ type_timestamp_with_timezone() }}) as createdate,
    cast(hs_msteams_message_id as {{ dbt_utils.type_string() }}) as hs_msteams_message_id,
    cast(hs_inbox_id as {{ dbt_utils.type_float() }}) as hs_inbox_id,
    cast({{ empty_string_to_null('hs_lastcontacted') }} as {{ type_timestamp_with_timezone() }}) as hs_lastcontacted,
    cast(time_to_first_agent_reply as {{ dbt_utils.type_float() }}) as time_to_first_agent_reply,
    cast(hs_originating_email_engagement_id as {{ dbt_utils.type_float() }}) as hs_originating_email_engagement_id,
    cast(hs_ticket_id as {{ dbt_utils.type_float() }}) as hs_ticket_id,
    cast(hubspot_owner_id as {{ dbt_utils.type_string() }}) as hubspot_owner_id,
    cast({{ empty_string_to_null('hs_lastactivitydate') }} as {{ type_timestamp_with_timezone() }}) as hs_lastactivitydate,
    cast(hs_object_source_id as {{ dbt_utils.type_string() }}) as hs_object_source_id,
    cast(hs_created_by_user_id as {{ dbt_utils.type_float() }}) as hs_created_by_user_id,
    cast(nps_follow_up_question_version as {{ dbt_utils.type_float() }}) as nps_follow_up_question_version,
    {{ cast_to_boolean('hs_in_helpdesk') }} as hs_in_helpdesk,
    cast({{ empty_string_to_null('hs_first_agent_message_sent_at') }} as {{ type_timestamp_with_timezone() }}) as hs_first_agent_message_sent_at,
    cast({{ empty_string_to_null('last_engagement_date') }} as {{ type_timestamp_with_timezone() }}) as last_engagement_date,
    {{ cast_to_boolean('hs_was_imported') }} as hs_was_imported,
    cast({{ empty_string_to_null('hs_nextactivitydate') }} as {{ type_timestamp_with_timezone() }}) as hs_nextactivitydate,
    cast({{ empty_string_to_null('hs_last_email_date') }} as {{ type_timestamp_with_timezone() }}) as hs_last_email_date,
    cast({{ empty_string_to_null('notes_last_updated') }} as {{ type_timestamp_with_timezone() }}) as notes_last_updated,
    cast(hs_primary_company_name as {{ dbt_utils.type_string() }}) as hs_primary_company_name,
    cast(source_ref as {{ dbt_utils.type_string() }}) as source_ref,
    cast(source_type as {{ dbt_utils.type_string() }}) as source_type,
    cast(hs_conversations_originating_message_id as {{ dbt_utils.type_string() }}) as hs_conversations_originating_message_id,
    cast(hs_pipeline_stage as {{ dbt_utils.type_string() }}) as hs_pipeline_stage,
    cast(created_by as {{ dbt_utils.type_float() }}) as created_by,
    cast(tags as {{ dbt_utils.type_string() }}) as tags,
    cast(hs_ticket_priority as {{ dbt_utils.type_string() }}) as hs_ticket_priority,
    cast(hs_custom_inbox as {{ dbt_utils.type_float() }}) as hs_custom_inbox,
    cast(nps_score as {{ dbt_utils.type_string() }}) as nps_score,
    cast(hs_external_object_ids as {{ dbt_utils.type_string() }}) as hs_external_object_ids,
    cast({{ empty_string_to_null('first_agent_reply_date') }} as {{ type_timestamp_with_timezone() }}) as first_agent_reply_date,
    cast(hs_num_times_contacted as {{ dbt_utils.type_float() }}) as hs_num_times_contacted,
    cast(hs_object_id as {{ dbt_utils.type_float() }}) as hs_object_id,
    cast(nps_follow_up_answer as {{ dbt_utils.type_string() }}) as nps_follow_up_answer,
    cast(hs_file_upload as {{ dbt_utils.type_string() }}) as hs_file_upload,
    cast(hs_merged_object_ids as {{ dbt_utils.type_string() }}) as hs_merged_object_ids,
    cast(hs_assignment_method as {{ dbt_utils.type_string() }}) as hs_assignment_method,
    cast({{ empty_string_to_null('last_reply_date') }} as {{ type_timestamp_with_timezone() }}) as last_reply_date,
    cast(source_thread_id as {{ dbt_utils.type_string() }}) as source_thread_id,
    cast(hs_most_relevant_sla_status as {{ dbt_utils.type_string() }}) as hs_most_relevant_sla_status,
    cast({{ empty_string_to_null('hs_sales_email_last_replied') }} as {{ type_timestamp_with_timezone() }}) as hs_sales_email_last_replied,
    cast(hs_updated_by_user_id as {{ dbt_utils.type_float() }}) as hs_updated_by_user_id,
    cast(hs_time_in_2 as {{ dbt_utils.type_float() }}) as hs_time_in_2,
    cast(hs_time_in_3 as {{ dbt_utils.type_float() }}) as hs_time_in_3,
    cast(hs_time_in_1 as {{ dbt_utils.type_float() }}) as hs_time_in_1,
    cast(hs_pipeline as {{ dbt_utils.type_string() }}) as hs_pipeline,
    cast(hs_all_team_ids as {{ dbt_utils.type_string() }}) as hs_all_team_ids,
    {{ cast_to_boolean('hs_read_only') }} as hs_read_only,
    cast(hs_time_in_4 as {{ dbt_utils.type_float() }}) as hs_time_in_4,
    cast({{ empty_string_to_null('hs_date_exited_1') }} as {{ type_timestamp_with_timezone() }}) as hs_date_exited_1,
    cast({{ empty_string_to_null('hs_date_exited_2') }} as {{ type_timestamp_with_timezone() }}) as hs_date_exited_2,
    cast({{ adapter.quote('content') }} as {{ dbt_utils.type_string() }}) as {{ adapter.quote('content') }},
    cast({{ empty_string_to_null('hs_date_exited_3') }} as {{ type_timestamp_with_timezone() }}) as hs_date_exited_3,
    cast(hs_primary_company as {{ dbt_utils.type_string() }}) as hs_primary_company,
    cast({{ empty_string_to_null('hs_date_exited_4') }} as {{ type_timestamp_with_timezone() }}) as hs_date_exited_4,
    cast(hs_last_email_activity as {{ dbt_utils.type_string() }}) as hs_last_email_activity,
    cast({{ empty_string_to_null('hs_helpdesk_sort_timestamp') }} as {{ type_timestamp_with_timezone() }}) as hs_helpdesk_sort_timestamp,
    cast(hubspot_team_id as {{ dbt_utils.type_string() }}) as hubspot_team_id,
    cast(hs_unique_creation_key as {{ dbt_utils.type_string() }}) as hs_unique_creation_key,
    cast({{ empty_string_to_null('hs_date_entered_4') }} as {{ type_timestamp_with_timezone() }}) as hs_date_entered_4,
    cast(num_contacted_notes as {{ dbt_utils.type_float() }}) as num_contacted_notes,
    cast(hs_primary_company_id as {{ dbt_utils.type_float() }}) as hs_primary_company_id,
    cast(hs_user_ids_of_all_notification_followers as {{ dbt_utils.type_string() }}) as hs_user_ids_of_all_notification_followers,
    cast({{ empty_string_to_null('hs_date_entered_3') }} as {{ type_timestamp_with_timezone() }}) as hs_date_entered_3,
    cast({{ empty_string_to_null('hs_last_message_sent_at') }} as {{ type_timestamp_with_timezone() }}) as hs_last_message_sent_at,
    cast({{ empty_string_to_null('hs_date_entered_2') }} as {{ type_timestamp_with_timezone() }}) as hs_date_entered_2,
    cast({{ empty_string_to_null('hs_createdate') }} as {{ type_timestamp_with_timezone() }}) as hs_createdate,
    cast({{ empty_string_to_null('hs_date_entered_1') }} as {{ type_timestamp_with_timezone() }}) as hs_date_entered_1,
    cast({{ empty_string_to_null('hs_last_message_received_at') }} as {{ type_timestamp_with_timezone() }}) as hs_last_message_received_at,
    cast(hs_thread_ids_to_restore as {{ dbt_utils.type_string() }}) as hs_thread_ids_to_restore,
    cast(hs_originating_generic_channel_id as {{ dbt_utils.type_string() }}) as hs_originating_generic_channel_id,
    cast(hs_latest_message_seen_by_agent_ids as {{ dbt_utils.type_string() }}) as hs_latest_message_seen_by_agent_ids,
    cast(hs_all_owner_ids as {{ dbt_utils.type_string() }}) as hs_all_owner_ids,
    {{ cast_to_boolean('hs_last_message_from_visitor') }} as hs_last_message_from_visitor,
    cast({{ empty_string_to_null('notes_next_activity_date') }} as {{ type_timestamp_with_timezone() }}) as notes_next_activity_date,
    cast(hs_conversations_originating_thread_id as {{ dbt_utils.type_float() }}) as hs_conversations_originating_thread_id,
    cast(hs_pinned_engagement_id as {{ dbt_utils.type_float() }}) as hs_pinned_engagement_id,
    cast(hs_most_relevant_sla_type as {{ dbt_utils.type_string() }}) as hs_most_relevant_sla_type,
    cast(hs_originating_channel_instance_id as {{ dbt_utils.type_string() }}) as hs_originating_channel_instance_id,
    cast(hs_resolution as {{ dbt_utils.type_string() }}) as hs_resolution,
    cast(hs_user_ids_of_all_owners as {{ dbt_utils.type_string() }}) as hs_user_ids_of_all_owners,
    cast(hs_ticket_category as {{ dbt_utils.type_string() }}) as hs_ticket_category,
    cast({{ empty_string_to_null('hs_lastmodifieddate') }} as {{ type_timestamp_with_timezone() }}) as hs_lastmodifieddate,
    cast({{ empty_string_to_null('notes_last_contacted') }} as {{ type_timestamp_with_timezone() }}) as notes_last_contacted,
    cast({{ empty_string_to_null('hubspot_owner_assigneddate') }} as {{ type_timestamp_with_timezone() }}) as hubspot_owner_assigneddate,
    cast(hs_num_associated_companies as {{ dbt_utils.type_float() }}) as hs_num_associated_companies,
    cast(hs_object_source as {{ dbt_utils.type_string() }}) as hs_object_source,
    cast(hs_object_source_user_id as {{ dbt_utils.type_float() }}) as hs_object_source_user_id,
    cast(time_to_close as {{ dbt_utils.type_float() }}) as time_to_close,
    cast(hs_auto_generated_from_thread_id as {{ dbt_utils.type_float() }}) as hs_auto_generated_from_thread_id,
    cast(num_notes as {{ dbt_utils.type_float() }}) as num_notes,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('tickets_properties_ab1') }}
-- properties at tickets/properties
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

