{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    schema = "_airbyte_public",
    tags = [ "nested-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ ref('tickets_scd') }}
select
    _airbyte_tickets_hashid,
    {{ json_extract_scalar('properties', ['closed_date'], ['closed_date']) }} as closed_date,
    {{ json_extract_scalar('properties', ['hs_all_accessible_team_ids'], ['hs_all_accessible_team_ids']) }} as hs_all_accessible_team_ids,
    {{ json_extract_scalar('properties', ['hs_all_conversation_mentions'], ['hs_all_conversation_mentions']) }} as hs_all_conversation_mentions,
    {{ json_extract_scalar('properties', ['hs_user_ids_of_all_notification_unfollowers'], ['hs_user_ids_of_all_notification_unfollowers']) }} as hs_user_ids_of_all_notification_unfollowers,
    {{ json_extract_scalar('properties', ['subject'], ['subject']) }} as subject,
    {{ json_extract_scalar('properties', ['createdate'], ['createdate']) }} as createdate,
    {{ json_extract_scalar('properties', ['hs_msteams_message_id'], ['hs_msteams_message_id']) }} as hs_msteams_message_id,
    {{ json_extract_scalar('properties', ['hs_inbox_id'], ['hs_inbox_id']) }} as hs_inbox_id,
    {{ json_extract_scalar('properties', ['hs_lastcontacted'], ['hs_lastcontacted']) }} as hs_lastcontacted,
    {{ json_extract_scalar('properties', ['time_to_first_agent_reply'], ['time_to_first_agent_reply']) }} as time_to_first_agent_reply,
    {{ json_extract_scalar('properties', ['hs_originating_email_engagement_id'], ['hs_originating_email_engagement_id']) }} as hs_originating_email_engagement_id,
    {{ json_extract_scalar('properties', ['hs_ticket_id'], ['hs_ticket_id']) }} as hs_ticket_id,
    {{ json_extract_scalar('properties', ['hubspot_owner_id'], ['hubspot_owner_id']) }} as hubspot_owner_id,
    {{ json_extract_scalar('properties', ['hs_lastactivitydate'], ['hs_lastactivitydate']) }} as hs_lastactivitydate,
    {{ json_extract_scalar('properties', ['hs_object_source_id'], ['hs_object_source_id']) }} as hs_object_source_id,
    {{ json_extract_scalar('properties', ['hs_created_by_user_id'], ['hs_created_by_user_id']) }} as hs_created_by_user_id,
    {{ json_extract_scalar('properties', ['nps_follow_up_question_version'], ['nps_follow_up_question_version']) }} as nps_follow_up_question_version,
    {{ json_extract_scalar('properties', ['hs_in_helpdesk'], ['hs_in_helpdesk']) }} as hs_in_helpdesk,
    {{ json_extract_scalar('properties', ['hs_first_agent_message_sent_at'], ['hs_first_agent_message_sent_at']) }} as hs_first_agent_message_sent_at,
    {{ json_extract_scalar('properties', ['last_engagement_date'], ['last_engagement_date']) }} as last_engagement_date,
    {{ json_extract_scalar('properties', ['hs_was_imported'], ['hs_was_imported']) }} as hs_was_imported,
    {{ json_extract_scalar('properties', ['hs_nextactivitydate'], ['hs_nextactivitydate']) }} as hs_nextactivitydate,
    {{ json_extract_scalar('properties', ['hs_last_email_date'], ['hs_last_email_date']) }} as hs_last_email_date,
    {{ json_extract_scalar('properties', ['notes_last_updated'], ['notes_last_updated']) }} as notes_last_updated,
    {{ json_extract_scalar('properties', ['hs_primary_company_name'], ['hs_primary_company_name']) }} as hs_primary_company_name,
    {{ json_extract_scalar('properties', ['source_ref'], ['source_ref']) }} as source_ref,
    {{ json_extract_scalar('properties', ['source_type'], ['source_type']) }} as source_type,
    {{ json_extract_scalar('properties', ['hs_conversations_originating_message_id'], ['hs_conversations_originating_message_id']) }} as hs_conversations_originating_message_id,
    {{ json_extract_scalar('properties', ['hs_pipeline_stage'], ['hs_pipeline_stage']) }} as hs_pipeline_stage,
    {{ json_extract_scalar('properties', ['created_by'], ['created_by']) }} as created_by,
    {{ json_extract_scalar('properties', ['tags'], ['tags']) }} as tags,
    {{ json_extract_scalar('properties', ['hs_ticket_priority'], ['hs_ticket_priority']) }} as hs_ticket_priority,
    {{ json_extract_scalar('properties', ['hs_custom_inbox'], ['hs_custom_inbox']) }} as hs_custom_inbox,
    {{ json_extract_scalar('properties', ['nps_score'], ['nps_score']) }} as nps_score,
    {{ json_extract_scalar('properties', ['hs_external_object_ids'], ['hs_external_object_ids']) }} as hs_external_object_ids,
    {{ json_extract_scalar('properties', ['first_agent_reply_date'], ['first_agent_reply_date']) }} as first_agent_reply_date,
    {{ json_extract_scalar('properties', ['hs_num_times_contacted'], ['hs_num_times_contacted']) }} as hs_num_times_contacted,
    {{ json_extract_scalar('properties', ['hs_object_id'], ['hs_object_id']) }} as hs_object_id,
    {{ json_extract_scalar('properties', ['nps_follow_up_answer'], ['nps_follow_up_answer']) }} as nps_follow_up_answer,
    {{ json_extract_scalar('properties', ['hs_file_upload'], ['hs_file_upload']) }} as hs_file_upload,
    {{ json_extract_scalar('properties', ['hs_merged_object_ids'], ['hs_merged_object_ids']) }} as hs_merged_object_ids,
    {{ json_extract_scalar('properties', ['hs_assignment_method'], ['hs_assignment_method']) }} as hs_assignment_method,
    {{ json_extract_scalar('properties', ['last_reply_date'], ['last_reply_date']) }} as last_reply_date,
    {{ json_extract_scalar('properties', ['source_thread_id'], ['source_thread_id']) }} as source_thread_id,
    {{ json_extract_scalar('properties', ['hs_most_relevant_sla_status'], ['hs_most_relevant_sla_status']) }} as hs_most_relevant_sla_status,
    {{ json_extract_scalar('properties', ['hs_sales_email_last_replied'], ['hs_sales_email_last_replied']) }} as hs_sales_email_last_replied,
    {{ json_extract_scalar('properties', ['hs_updated_by_user_id'], ['hs_updated_by_user_id']) }} as hs_updated_by_user_id,
    {{ json_extract_scalar('properties', ['hs_time_in_2'], ['hs_time_in_2']) }} as hs_time_in_2,
    {{ json_extract_scalar('properties', ['hs_time_in_3'], ['hs_time_in_3']) }} as hs_time_in_3,
    {{ json_extract_scalar('properties', ['hs_time_in_1'], ['hs_time_in_1']) }} as hs_time_in_1,
    {{ json_extract_scalar('properties', ['hs_pipeline'], ['hs_pipeline']) }} as hs_pipeline,
    {{ json_extract_scalar('properties', ['hs_all_team_ids'], ['hs_all_team_ids']) }} as hs_all_team_ids,
    {{ json_extract_scalar('properties', ['hs_read_only'], ['hs_read_only']) }} as hs_read_only,
    {{ json_extract_scalar('properties', ['hs_time_in_4'], ['hs_time_in_4']) }} as hs_time_in_4,
    {{ json_extract_scalar('properties', ['hs_date_exited_1'], ['hs_date_exited_1']) }} as hs_date_exited_1,
    {{ json_extract_scalar('properties', ['hs_date_exited_2'], ['hs_date_exited_2']) }} as hs_date_exited_2,
    {{ json_extract_scalar('properties', ['content'], ['content']) }} as {{ adapter.quote('content') }},
    {{ json_extract_scalar('properties', ['hs_date_exited_3'], ['hs_date_exited_3']) }} as hs_date_exited_3,
    {{ json_extract_scalar('properties', ['hs_primary_company'], ['hs_primary_company']) }} as hs_primary_company,
    {{ json_extract_scalar('properties', ['hs_date_exited_4'], ['hs_date_exited_4']) }} as hs_date_exited_4,
    {{ json_extract_scalar('properties', ['hs_last_email_activity'], ['hs_last_email_activity']) }} as hs_last_email_activity,
    {{ json_extract_scalar('properties', ['hs_helpdesk_sort_timestamp'], ['hs_helpdesk_sort_timestamp']) }} as hs_helpdesk_sort_timestamp,
    {{ json_extract_scalar('properties', ['hubspot_team_id'], ['hubspot_team_id']) }} as hubspot_team_id,
    {{ json_extract_scalar('properties', ['hs_unique_creation_key'], ['hs_unique_creation_key']) }} as hs_unique_creation_key,
    {{ json_extract_scalar('properties', ['hs_date_entered_4'], ['hs_date_entered_4']) }} as hs_date_entered_4,
    {{ json_extract_scalar('properties', ['num_contacted_notes'], ['num_contacted_notes']) }} as num_contacted_notes,
    {{ json_extract_scalar('properties', ['hs_primary_company_id'], ['hs_primary_company_id']) }} as hs_primary_company_id,
    {{ json_extract_scalar('properties', ['hs_user_ids_of_all_notification_followers'], ['hs_user_ids_of_all_notification_followers']) }} as hs_user_ids_of_all_notification_followers,
    {{ json_extract_scalar('properties', ['hs_date_entered_3'], ['hs_date_entered_3']) }} as hs_date_entered_3,
    {{ json_extract_scalar('properties', ['hs_last_message_sent_at'], ['hs_last_message_sent_at']) }} as hs_last_message_sent_at,
    {{ json_extract_scalar('properties', ['hs_date_entered_2'], ['hs_date_entered_2']) }} as hs_date_entered_2,
    {{ json_extract_scalar('properties', ['hs_createdate'], ['hs_createdate']) }} as hs_createdate,
    {{ json_extract_scalar('properties', ['hs_date_entered_1'], ['hs_date_entered_1']) }} as hs_date_entered_1,
    {{ json_extract_scalar('properties', ['hs_last_message_received_at'], ['hs_last_message_received_at']) }} as hs_last_message_received_at,
    {{ json_extract_scalar('properties', ['hs_thread_ids_to_restore'], ['hs_thread_ids_to_restore']) }} as hs_thread_ids_to_restore,
    {{ json_extract_scalar('properties', ['hs_originating_generic_channel_id'], ['hs_originating_generic_channel_id']) }} as hs_originating_generic_channel_id,
    {{ json_extract_scalar('properties', ['hs_latest_message_seen_by_agent_ids'], ['hs_latest_message_seen_by_agent_ids']) }} as hs_latest_message_seen_by_agent_ids,
    {{ json_extract_scalar('properties', ['hs_all_owner_ids'], ['hs_all_owner_ids']) }} as hs_all_owner_ids,
    {{ json_extract_scalar('properties', ['hs_last_message_from_visitor'], ['hs_last_message_from_visitor']) }} as hs_last_message_from_visitor,
    {{ json_extract_scalar('properties', ['notes_next_activity_date'], ['notes_next_activity_date']) }} as notes_next_activity_date,
    {{ json_extract_scalar('properties', ['hs_conversations_originating_thread_id'], ['hs_conversations_originating_thread_id']) }} as hs_conversations_originating_thread_id,
    {{ json_extract_scalar('properties', ['hs_pinned_engagement_id'], ['hs_pinned_engagement_id']) }} as hs_pinned_engagement_id,
    {{ json_extract_scalar('properties', ['hs_most_relevant_sla_type'], ['hs_most_relevant_sla_type']) }} as hs_most_relevant_sla_type,
    {{ json_extract_scalar('properties', ['hs_originating_channel_instance_id'], ['hs_originating_channel_instance_id']) }} as hs_originating_channel_instance_id,
    {{ json_extract_scalar('properties', ['hs_resolution'], ['hs_resolution']) }} as hs_resolution,
    {{ json_extract_scalar('properties', ['hs_user_ids_of_all_owners'], ['hs_user_ids_of_all_owners']) }} as hs_user_ids_of_all_owners,
    {{ json_extract_scalar('properties', ['hs_ticket_category'], ['hs_ticket_category']) }} as hs_ticket_category,
    {{ json_extract_scalar('properties', ['hs_lastmodifieddate'], ['hs_lastmodifieddate']) }} as hs_lastmodifieddate,
    {{ json_extract_scalar('properties', ['notes_last_contacted'], ['notes_last_contacted']) }} as notes_last_contacted,
    {{ json_extract_scalar('properties', ['hubspot_owner_assigneddate'], ['hubspot_owner_assigneddate']) }} as hubspot_owner_assigneddate,
    {{ json_extract_scalar('properties', ['hs_num_associated_companies'], ['hs_num_associated_companies']) }} as hs_num_associated_companies,
    {{ json_extract_scalar('properties', ['hs_object_source'], ['hs_object_source']) }} as hs_object_source,
    {{ json_extract_scalar('properties', ['hs_object_source_user_id'], ['hs_object_source_user_id']) }} as hs_object_source_user_id,
    {{ json_extract_scalar('properties', ['time_to_close'], ['time_to_close']) }} as time_to_close,
    {{ json_extract_scalar('properties', ['hs_auto_generated_from_thread_id'], ['hs_auto_generated_from_thread_id']) }} as hs_auto_generated_from_thread_id,
    {{ json_extract_scalar('properties', ['num_notes'], ['num_notes']) }} as num_notes,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('tickets_scd') }} as table_alias
-- properties at tickets/properties
where 1 = 1
and properties is not null
{{ incremental_clause('_airbyte_emitted_at', this) }}

