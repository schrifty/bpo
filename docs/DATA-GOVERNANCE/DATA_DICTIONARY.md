# Data Dictionary

## config cohorts.yaml
- `cohorts_yaml`

## config pendo_orphans.yaml
- `pendo_orphans_yaml`

## config/teams.yaml
- `teams_yaml.customer_team`
- `teams_yaml.leandna_site_ids`
- `teams_yaml.leandna_team`

## CSR
- `csr.platform_health`
- `csr.platform_health.customer`
- `csr.platform_health.error`
- `csr.platform_health.factory_count`
- `csr.platform_health.health_distribution`
- `csr.platform_health.sites`
- `csr.platform_health.sites[]`
- `csr.platform_health.source`
- `csr.platform_health.total_critical_shortages`
- `csr.platform_health.total_shortages`
- `csr.platform_value.customer`
- `csr.platform_value.error`
- `csr.platform_value.factory_count`
- `csr.platform_value.sites`
- `csr.platform_value.sites[]`
- `csr.platform_value.source`
- `csr.platform_value.total_current_fy_spend`
- `csr.platform_value.total_current_week52_ldna_target`
- `csr.platform_value.total_ia_current_period_open_value`
- `csr.platform_value.total_ia_previous_period_savings`
- `csr.platform_value.total_open_ia_value`
- `csr.platform_value.total_overdue_tasks`
- `csr.platform_value.total_pos_placed_30d`
- `csr.platform_value.total_potential_savings`
- `csr.platform_value.total_potential_to_sell`
- `csr.platform_value.total_previous_fy_spend`
- `csr.platform_value.total_recs_created_30d`
- `csr.platform_value.total_savings`
- `csr.supply_chain.customer`
- `csr.supply_chain.error`
- `csr.supply_chain.factory_count`
- `csr.supply_chain.sites`
- `csr.supply_chain.sites[]`
- `csr.supply_chain.source`
- `csr.supply_chain.totals.early_deliveries`
- `csr.supply_chain.totals.excess_on_hand`
- `csr.supply_chain.totals.excess_on_order`
- `csr.supply_chain.totals.excess_on_order_obsolete`
- `csr.supply_chain.totals.excess_onhand_demanded`
- `csr.supply_chain.totals.excess_onhand_obsolete`
- `csr.supply_chain.totals.manufactured_inventory`
- `csr.supply_chain.totals.on_hand`
- `csr.supply_chain.totals.on_order`
- `csr.supply_chain.totals.past_due_po`
- `csr.supply_chain.totals.past_due_req`

## CSR site row
- `csr.platform_health.sites[]`
- `csr.platform_value.sites[]`
- `csr.supply_chain.sites[]`
- Export markdown / spreadsheet / portfolio §4 present these with **CSR display labels** from `config/cs_report_column_labels.yaml` (e.g. `Current shortages (purchased)` for `shortageItemCount`); internal APIs keep snake_case keys.

## CSR→summary
- `platform_value.factory_count`
- `platform_value.total_current_fy_spend`
- `platform_value.total_current_week52_ldna_target`
- `platform_value.total_ia_current_period_open_value`
- `platform_value.total_ia_previous_period_savings`
- `platform_value.total_open_ia_value`
- `platform_value.total_overdue_tasks`
- `platform_value.total_pos_placed_30d`
- `platform_value.total_potential_savings`
- `platform_value.total_potential_to_sell`
- `platform_value.total_previous_fy_spend`
- `platform_value.total_recs_created_30d`
- `platform_value.total_savings`
- `supply_chain.factory_count`
- `supply_chain.totals.early_deliveries`
- `supply_chain.totals.excess_on_hand`
- `supply_chain.totals.excess_on_order`
- `supply_chain.totals.excess_on_order_obsolete`
- `supply_chain.totals.excess_onhand_demanded`
- `supply_chain.totals.excess_onhand_obsolete`
- `supply_chain.totals.manufactured_inventory`
- `supply_chain.totals.on_hand`
- `supply_chain.totals.on_order`
- `supply_chain.totals.past_due_po`
- `supply_chain.totals.past_due_req`

## data_summary
- `account_avg_weekly_hours`
- `account_total_minutes`
- `active_sites`
- `active_users`
- `cs_health_sites`
- `cs_health_sites.ctb`
- `cs_health_sites.ctc`
- `cs_health_sites.health`
- `cs_health_sites.site`
- `customer_name`
- `health_score`
- `platform_value`
- `report_date`
- `site_details`
- `supply_chain`
- `support.open`
- `support.resolved`
- `support.total_tickets`
- `total_critical_shortages`
- `total_shortages`
- `total_sites`
- `total_users`
- `total_visitors`
- `unique_visitors`
- `weekly_active_buyers_pct_avg`

## GitHub
- `github`
- `github_productivity`
- `github_productivity.by_email`
- `github_productivity.company_engineers`
- `github_productivity.weekly`
- `github_productivity.takeaways`
- `engineer_identity`
- `ai_productivity`
- `ai_productivity.company`
- `ai_productivity.by_email`
- `ai_productivity.weekly_trend`
- `ai_productivity.quadrant_counts`
- `ai_productivity.top_yield`
- `ai_productivity.review`
- `ai_productivity.takeaways`

## Cursor (Admin API)
- `cursor_usage`
- `cursor_usage.configured`
- `cursor_usage.generated_at`
- `cursor_usage.window_days`
- `cursor_usage.warnings`
- `cursor_usage.errors`
- `cursor_usage.totals.included_spend_cents_cycle`
- `cursor_usage.cost_engineers.totals.included_spend_cents_cycle`
- `cursor_usage.engineer_usage_by_email`

## Deck governance
- `_governance`
- `_governance.deck_id`
- `_governance.assembled_at`
- `_governance.source_status`
- `_governance.scope`
- `_governance.freshness`
- `_governance.lineage`
- `_governance.discrepancies`
- `_governance.cross_checks`
- `_governance.authority_footnote`

## Slack
- `slack.source`
- `slack.customer`
- `slack.days`
- `slack.configured`
- `slack.channels_matched`
- `slack.conversation_summaries`
- `slack.conversation_summaries[].channel_id`
- `slack.conversation_summaries[].channel_name`
- `slack.conversation_summaries[].message_count`
- `slack.conversation_summaries[].summary_lines`
- `slack.conversation_summaries[].summary_text`
- `slack.combined_summary_markdown`
- `slack.note`
- `slack.error`
- `slack.skipped`
- `slack.lookback_days`
- `slack.top_n`
- `slack.customers`
- `slack.customers[].llm_summary`
- `slack.customers[].llm_summary.summary_markdown`
- `slack.customers[].llm_summary.themes`
- `slack.customers[].llm_summary.open_items`
- `slack.customers[].llm_summary.sentiment`
- `_llm_export_slack.performance`

## internal
- `_drive_svc`
- `_hydrate_slide_hints`
- `_signals_llm_manifest_rules`
- `_signals_llm_slide_prompt`
- `_slide_plan`
- `_slides_svc`

## Jira
- `jira.by_priority`
- `jira.by_request_type`
- `jira.by_sentiment`
- `jira.by_status`
- `jira.by_type`
- `jira.customer`
- `jira.customer_ticket_metrics`
- `jira.support_kpis` (HELP operational KPI bundle: intake, flow, backlog, SLA, tail risk, customer health, aging thresholds)
- `jira.help_factory_start_day_buckets`
- `jira.help_monthly_operational_metrics` (monthly rows include HELP vs Outage/Healthcheck bands; `outage_resolved` and `outage_delta` for HC slice)
- `jira.days`
- `jira.engineering`
- `jira.enhancements`
- `jira.escalated`
- `jira.escalated_issues`
- `jira.help_scope`
- `jira.jql_queries`
- `jira.jsm_organizations_resolved`
- `jira.open_bugs`
- `jira.open_issues`
- `jira.recent_issues`
- `jira.resolved_issues`
- `jira.tickets_over_time`
- `jira.total_issues`
- `jira.ttfr`
- `jira.ttr`

## LeanDNA
- `leandna_item_master.abc_distribution`
- `leandna_item_master.doi_backwards`
- `leandna_item_master.enabled`
- `leandna_item_master.error`
- `leandna_item_master.excess_breakdown`
- `leandna_item_master.high_risk_items`
- `leandna_item_master.item_count`
- `leandna_item_master.lead_time_variance`
- `leandna_item_master.reason`
- `leandna_item_master.sites_requested`
- `leandna_lean_projects.active_projects`
- `leandna_lean_projects.best_practice_count`
- `leandna_lean_projects.data_fetched_at`
- `leandna_lean_projects.enabled`
- `leandna_lean_projects.error`
- `leandna_lean_projects.executive_sponsor`
- `leandna_lean_projects.monthly_savings`
- `leandna_lean_projects.quarter_end`
- `leandna_lean_projects.quarter_start`
- `leandna_lean_projects.reason`
- `leandna_lean_projects.savings_achievement_pct`
- `leandna_lean_projects.stage_distribution`
- `leandna_lean_projects.state_distribution`
- `leandna_lean_projects.top_projects`
- `leandna_lean_projects.total_projects`
- `leandna_lean_projects.total_savings_actual`
- `leandna_lean_projects.total_savings_target`
- `leandna_lean_projects.validated_results_count`
- `leandna_shortage_trends.critical_items`
- `leandna_shortage_trends.critical_timeline`
- `leandna_shortage_trends.data_fetched_at`
- `leandna_shortage_trends.enabled`
- `leandna_shortage_trends.error`
- `leandna_shortage_trends.forecast`
- `leandna_shortage_trends.reason`
- `leandna_shortage_trends.scheduled_deliveries`
- `leandna_shortage_trends.total_items_in_shortage`
- `leandna_shortage_trends.weeks_forward`

## LeanDNA API
- `leandna_data_api.lean_project.sponsor`

## LeanDNA Data API
- `leandna_data_api.data_share`
- `leandna_data_api.identity`
- `leandna_data_api.inventory.purchased`
- `leandna_data_api.item_master`
- `leandna_data_api.lean_project.areas`
- `leandna_data_api.lean_project.categories`
- `leandna_data_api.lean_project.issues`
- `leandna_data_api.lean_project.list`
- `leandna_data_api.lean_project.savings`
- `leandna_data_api.lean_project.stage_history`
- `leandna_data_api.lean_project.tasks`
- `leandna_data_api.lean_project.types`
- `leandna_data_api.material_shortages.by_order`
- `leandna_data_api.material_shortages.daily_by_item`
- `leandna_data_api.material_shortages.monthly_by_item`
- `leandna_data_api.material_shortages.scheduled_deliveries_daily`
- `leandna_data_api.material_shortages.scheduled_deliveries_monthly`
- `leandna_data_api.material_shortages.scheduled_deliveries_weekly`
- `leandna_data_api.material_shortages.weekly_by_item`
- `leandna_data_api.metric.definitions`
- `leandna_data_api.metric.report`
- `leandna_data_api.metric.metric_data_point`
- `leandna_data_api.lean_project.mutations`
- `leandna_data_api.supply_order.purchase_order`
- `leandna_data_api.writeback.purchase_order_actions`
- `leandna_data_api.writeback.transition_actions`
- `leandna_item_master`
- `leandna_lean_projects`
- `leandna_shortage_trends`

## Pendo
- `account.account_id`
- `account.csm`
- `account.internal_visitors`
- `account.name`
- `account.region`
- `account.total_sites`
- `account.total_visitors`
- `at_risk_users`
- `at_risk_users[].days_inactive`
- `at_risk_users[].email`
- `at_risk_users[].language`
- `at_risk_users[].last_visit`
- `at_risk_users[].role`
- `benchmarks.cohort`
- `benchmarks.cohort_count`
- `benchmarks.cohort_median_rate`
- `benchmarks.cohort_name`
- `benchmarks.customer_active_rate`
- `benchmarks.peer_count`
- `benchmarks.peer_median_rate`
- `champions`
- `champions[].days_inactive`
- `champions[].email`
- `champions[].language`
- `champions[].last_visit`
- `champions[].role`
- `engagement.active_30d`
- `engagement.active_7d`
- `engagement.active_rate_7d`
- `engagement.dormant`
- `engagement.role_active`
- `engagement.role_dormant`
- `feature_adoption_insights`
- `frustration`
- `pendo_catalog_appendix`
- `pendo.visitors[].metadata.agent.emailaddress`
- `pendo.visitors[].metadata.agent.isinternaluser`
- `pendo.visitors[].metadata.agent.language`
- `pendo.visitors[].metadata.agent.role`
- `pendo.visitors[].metadata.auto.lastvisit`
- `pendo.visitors[].visitorId`
- `poll_events`
- `signals`
- `signals_trend_context`
- `sites`
- `top_features`
- `top_pages`
- `track_events_breakdown`
- `visitor_languages`

## Pendo depth
- `depth.active_users`
- `depth.breakdown`
- `depth.collab_events`
- `depth.customer`
- `depth.days`
- `depth.read_events`
- `depth.total_feature_events`
- `depth.write_events`
- `depth.write_ratio`

## Pendo exports
- `exports.active_users`
- `exports.by_feature`
- `exports.customer`
- `exports.days`
- `exports.exports_per_active_user`
- `exports.top_exporters`
- `exports.total_exports`

## Pendo guides
- `guides.advance_rate`
- `guides.advanced`
- `guides.customer`
- `guides.days`
- `guides.dismiss_rate`
- `guides.dismissed`
- `guides.guide_reach`
- `guides.seen`
- `guides.top_guides`
- `guides.total_guide_events`
- `guides.total_visitors`
- `guides.users_who_saw_guides`

## Pendo Kei
- `kei.active_users`
- `kei.adoption_rate`
- `kei.customer`
- `kei.days`
- `kei.executive_queries`
- `kei.executive_users`
- `kei.total_queries`
- `kei.unique_users`
- `kei.users`

## Pendo→summary
- `site_details.events`
- `site_details.features_used`
- `site_details.last_active`
- `site_details.name`
- `site_details.pages_used`
- `site_details.total_minutes`
- `site_details.visitors`

## portfolio
- `portfolio.cohort_digest`
- `portfolio.cohort_findings_bullets`
- `portfolio.customer_count`
- `portfolio.customers`
- `portfolio.days`
- `portfolio.generated`
- `portfolio.portfolio_leaders`
- `portfolio.portfolio_signals`
- `portfolio.portfolio_trends`
- `portfolio.type`

## portfolio row
- `portfolio.customers[]`

## report (Salesforce portfolio enrichments on deck report root)
- `portfolio_expansion_book.active_customers_with_expansion_wins_cy`
- `portfolio_expansion_book.active_customers_with_new_business_won_cy`
- `portfolio_expansion_book.calendar_year`
- `portfolio_expansion_book.closed_won_expansion_amount_sum_cy`
- `portfolio_expansion_book.closed_won_expansion_deal_count_cy`
- `portfolio_expansion_book.configured`
- `portfolio_expansion_book.distinct_accounts_expansion_win_cy`
- `portfolio_expansion_book.distinct_accounts_new_business_win_cy`
- `portfolio_expansion_book.eligible_active_customer_count`
- `portfolio_expansion_book.empty`
- `portfolio_expansion_book.error`
- `portfolio_expansion_book.expanding_customer_labels_sample`
- `portfolio_expansion_book.pct_active_customers_expanding_cy`
- `portfolio_revenue_book.expansion_kpis`

## Portfolio revenue book (`portfolio_revenue_book`)

Commercial classification and ARR rollups for Salesforce Customer Entity reporting groups. See [`Cortex Export - User Guide.md`](../Cortex%20Export%20-%20User%20Guide.md).

- `portfolio_revenue_book.matched_customer_contract_rollups[].commercial_status` — `ACTIVE` | `OUT_OF_CONTRACT_RENEWING` | `CHURNED` | `FUTURE`
- `portfolio_revenue_book.matched_customer_contract_rollups[].active_arr`
- `portfolio_revenue_book.matched_customer_contract_rollups[].renewal_arr`
- `portfolio_revenue_book.matched_customer_contract_rollups[].current_arr` — `active_arr + renewal_arr` (ranking key)
- `portfolio_revenue_book.matched_customer_contract_rollups[].historical_arr`
- `portfolio_revenue_book.active_arr` / `renewal_arr` / `current_arr` / `historical_arr` — book totals
- `portfolio_revenue_book.future_contract_arr` / `future_customer_count`

**Caveat:** boolean `active` on rollups is **deprecated**; use `commercial_status` and `current_arr` for executive views.

## LLM export CS Report (`csr`, §4 — top customers by ARR)
- `csr.scope`
- `csr.top_n`
- `csr.selection_ranked`
- `csr.customers`
- `csr.customers[].salesforce_label`
- `csr.customers[].arr`
- `csr.customers[].csr_lookup_name`
- `csr.customers[].platform_health`
- `csr.customers[].supply_chain`
- `csr.customers[].platform_value`

## LLM export (`salesforce_comprehensive_portfolio`, §3c)
- `salesforce_comprehensive_portfolio`
- `salesforce_comprehensive_portfolio.by_customer`
- `salesforce_comprehensive_portfolio.entity_accounts`
- `salesforce_comprehensive_portfolio.entity_accounts[].division_group` (SF hierarchy: ultimate parent → parent → account name)
- `salesforce_comprehensive_portfolio.entity_accounts[].corporate_group` (corporate rollup label; `config/salesforce_reporting_rollups.yaml`)
- `salesforce_comprehensive_portfolio.entity_accounts[].ultimate_parent_group` (Ultimate Parent rollup; falls back to name parenthetical / corporate group when `ultimate_parent_name` blank)
- `salesforce_comprehensive_portfolio.entity_accounts_count`
- `salesforce_comprehensive_portfolio.arr_by_ultimate_parent` (ultimate-parent rollup from **portfolio contract rollups**, sorted by `current_arr` desc; all commercial_status segments)
- `salesforce_comprehensive_portfolio.arr_by_ultimate_parent[].ultimate_parent`
- `salesforce_comprehensive_portfolio.arr_by_ultimate_parent[].salesforce_labels`
- `salesforce_comprehensive_portfolio.arr_by_ultimate_parent[].arr` (alias of `historical_arr`)
- `salesforce_comprehensive_portfolio.arr_by_ultimate_parent[].historical_arr`
- `salesforce_comprehensive_portfolio.arr_by_ultimate_parent[].active_arr`
- `salesforce_comprehensive_portfolio.arr_by_ultimate_parent[].renewal_arr`
- `salesforce_comprehensive_portfolio.arr_by_ultimate_parent[].current_arr`
- `salesforce_comprehensive_portfolio.arr_by_ultimate_parent[].commercial_status`
- `salesforce_comprehensive_portfolio.arr_by_ultimate_parent[].entity_count`
- `salesforce_comprehensive_portfolio.arr_by_ultimate_parent[].active` (legacy current-book flag from `commercial_status`)
- `salesforce_comprehensive_portfolio.arr_by_ultimate_parent[].entity_names_sample`
- `salesforce_comprehensive_portfolio.portfolio_expansion_book`
- `salesforce_comprehensive_portfolio.row_limit`
- `salesforce_comprehensive_portfolio.customer_count`
- `salesforce_comprehensive_portfolio.configured`
- `salesforce.expansion_kpis`
- `salesforce.portfolio_expansion_book`

Per-customer values under `by_customer` reuse the same element paths as `salesforce.*` comprehensive payloads (`salesforce.categories`, `salesforce.accounts`, etc.).

## QBR
- `executive_sponsor`
- `leandna_exec_sponsor`

## report
- `customer`
- `customer_key_type`
- `days`
- `generated`
- `quarter`
- `quarter_end`
- `quarter_start`
- `salesforce_primary_account_id`

## Salesforce
- `salesforce.customer`
- `salesforce.error`
- `salesforce.matched`
- `salesforce.resolution`
- `salesforce.primary_account_id`
- `salesforce.account_ids`
- `salesforce.account_ids_expanded`
- `salesforce.opportunity_count_this_year`
- `salesforce.pipeline_arr`
- `salesforce.row_limit`
- `salesforce.category_errors`
- `salesforce.accounts[].Id`
- `salesforce.accounts[].Name`
- `salesforce.accounts[].LeanDNA_Entity_Name__c`
- `salesforce.accounts[].US_Persons_Only_Customer__c`
- `salesforce.accounts[].Contract_Status__c`
- `salesforce.accounts[].factory_start_date` (from `SF_ACCOUNT_FACTORY_START_DATE_FIELD`, default `Effective_Date_of_Order__c`)
- `salesforce.accounts[].Contract_Contract_Start_Date__c`
- `salesforce.accounts[].Contract_Contract_End_Date__c`
- `salesforce.accounts[].ARR__c`
- `salesforce.accounts[].ParentId`
- `salesforce.accounts[].parent_name`
- `salesforce.accounts[].ultimate_parent_name`
- `salesforce.categories.contacts`
- `salesforce.categories.opportunities`
- `salesforce.categories.opportunity_line_items`
- `salesforce.categories.cases`
- `salesforce.categories.tasks`
- `salesforce.categories.events`
- `salesforce.categories.contracts`
- `salesforce.categories.orders`
- `salesforce.categories.quotes`
- `salesforce.categories.assets`
- `salesforce.categories.owners_sample`
- `salesforce.categories.campaign_members`
- `salesforce.categories.campaigns_related`
- `salesforce.categories.leads_name_match`
- `salesforce.categories.products_org_sample`
- `salesforce.categories.pricebooks_org_sample`
- `salesforce.categories.contacts[].Id`
- `salesforce.categories.contacts[].FirstName`
- `salesforce.categories.contacts[].LastName`
- `salesforce.categories.contacts[].Email`
- `salesforce.categories.contacts[].Phone`
- `salesforce.categories.contacts[].AccountId`
- `salesforce.categories.contacts[].Title`
- `salesforce.categories.contacts[].MailingCity`
- `salesforce.categories.contacts[].MailingState`
- `salesforce.categories.contacts[].MailingCountry`
- `salesforce.categories.contacts[].OwnerId`
- `salesforce.categories.contacts[].CreatedDate`
- `salesforce.categories.opportunities[].Id`
- `salesforce.categories.opportunities[].Name`
- `salesforce.categories.opportunities[].AccountId`
- `salesforce.categories.opportunities[].StageName`
- `salesforce.categories.opportunities[].Amount`
- `salesforce.categories.opportunities[].Probability`
- `salesforce.categories.opportunities[].CloseDate`
- `salesforce.categories.opportunities[].Type`
- `salesforce.categories.opportunities[].ForecastCategoryName`
- `salesforce.categories.opportunities[].OwnerId`
- `salesforce.categories.opportunities[].CreatedDate`
- `salesforce.categories.opportunities[].LastModifiedDate`
- `salesforce.categories.opportunity_line_items[].Id`
- `salesforce.categories.opportunity_line_items[].OpportunityId`
- `salesforce.categories.opportunity_line_items[].Product2Id`
- `salesforce.categories.opportunity_line_items[].Quantity`
- `salesforce.categories.opportunity_line_items[].UnitPrice`
- `salesforce.categories.opportunity_line_items[].TotalPrice`
- `salesforce.categories.opportunity_line_items[].ServiceDate`
- `salesforce.categories.cases[].Id`
- `salesforce.categories.cases[].CaseNumber`
- `salesforce.categories.cases[].Subject`
- `salesforce.categories.cases[].Status`
- `salesforce.categories.cases[].Priority`
- `salesforce.categories.cases[].Origin`
- `salesforce.categories.cases[].AccountId`
- `salesforce.categories.cases[].ContactId`
- `salesforce.categories.cases[].OwnerId`
- `salesforce.categories.cases[].CreatedDate`
- `salesforce.categories.cases[].ClosedDate`
- `salesforce.categories.tasks[].Id`
- `salesforce.categories.tasks[].Subject`
- `salesforce.categories.tasks[].Status`
- `salesforce.categories.tasks[].Priority`
- `salesforce.categories.tasks[].ActivityDate`
- `salesforce.categories.tasks[].WhoId`
- `salesforce.categories.tasks[].WhatId`
- `salesforce.categories.tasks[].OwnerId`
- `salesforce.categories.tasks[].IsClosed`
- `salesforce.categories.tasks[].CreatedDate`
- `salesforce.categories.events[].Id`
- `salesforce.categories.events[].Subject`
- `salesforce.categories.events[].StartDateTime`
- `salesforce.categories.events[].EndDateTime`
- `salesforce.categories.events[].Location`
- `salesforce.categories.events[].WhoId`
- `salesforce.categories.events[].WhatId`
- `salesforce.categories.events[].OwnerId`
- `salesforce.categories.events[].CreatedDate`
- `salesforce.categories.contracts[].Id`
- `salesforce.categories.contracts[].ContractNumber`
- `salesforce.categories.contracts[].AccountId`
- `salesforce.categories.contracts[].Status`
- `salesforce.categories.contracts[].StartDate`
- `salesforce.categories.contracts[].EndDate`
- `salesforce.categories.contracts[].ContractTerm`
- `salesforce.categories.contracts[].OwnerId`
- `salesforce.categories.contracts[].CreatedDate`
- `salesforce.categories.orders[].Id`
- `salesforce.categories.orders[].OrderNumber`
- `salesforce.categories.orders[].AccountId`
- `salesforce.categories.orders[].EffectiveDate`
- `salesforce.categories.orders[].Status`
- `salesforce.categories.orders[].TotalAmount`
- `salesforce.categories.orders[].Type`
- `salesforce.categories.orders[].OwnerId`
- `salesforce.categories.orders[].CreatedDate`
- `salesforce.categories.quotes[].Id`
- `salesforce.categories.quotes[].QuoteNumber`
- `salesforce.categories.quotes[].Name`
- `salesforce.categories.quotes[].OpportunityId`
- `salesforce.categories.quotes[].AccountId`
- `salesforce.categories.quotes[].Status`
- `salesforce.categories.quotes[].ExpirationDate`
- `salesforce.categories.quotes[].GrandTotal`
- `salesforce.categories.quotes[].OwnerId`
- `salesforce.categories.quotes[].CreatedDate`
- `salesforce.categories.assets[].Id`
- `salesforce.categories.assets[].Name`
- `salesforce.categories.assets[].AccountId`
- `salesforce.categories.assets[].SerialNumber`
- `salesforce.categories.assets[].Status`
- `salesforce.categories.assets[].Product2Id`
- `salesforce.categories.assets[].InstallDate`
- `salesforce.categories.assets[].OwnerId`
- `salesforce.categories.owners_sample[].Id`
- `salesforce.categories.owners_sample[].Name`
- `salesforce.categories.owners_sample[].Username`
- `salesforce.categories.owners_sample[].Email`
- `salesforce.categories.owners_sample[].IsActive`
- `salesforce.categories.owners_sample[].ProfileId`
- `salesforce.categories.owners_sample[].UserType`
- `salesforce.categories.campaign_members[].Id`
- `salesforce.categories.campaign_members[].CampaignId`
- `salesforce.categories.campaign_members[].LeadId`
- `salesforce.categories.campaign_members[].ContactId`
- `salesforce.categories.campaign_members[].Status`
- `salesforce.categories.campaign_members[].CreatedDate`
- `salesforce.categories.campaigns_related[].Id`
- `salesforce.categories.campaigns_related[].Name`
- `salesforce.categories.campaigns_related[].Status`
- `salesforce.categories.campaigns_related[].Type`
- `salesforce.categories.campaigns_related[].StartDate`
- `salesforce.categories.campaigns_related[].EndDate`
- `salesforce.categories.campaigns_related[].BudgetedCost`
- `salesforce.categories.campaigns_related[].ActualCost`
- `salesforce.categories.campaigns_related[].OwnerId`
- `salesforce.categories.leads_name_match[].Id`
- `salesforce.categories.leads_name_match[].FirstName`
- `salesforce.categories.leads_name_match[].LastName`
- `salesforce.categories.leads_name_match[].Company`
- `salesforce.categories.leads_name_match[].Email`
- `salesforce.categories.leads_name_match[].Phone`
- `salesforce.categories.leads_name_match[].Status`
- `salesforce.categories.leads_name_match[].LeadSource`
- `salesforce.categories.leads_name_match[].OwnerId`
- `salesforce.categories.leads_name_match[].CreatedDate`
- `salesforce.categories.leads_name_match[].LastModifiedDate`
- `salesforce.categories.leads_name_match[].IsConverted`
- `salesforce.categories.products_org_sample[].Id`
- `salesforce.categories.products_org_sample[].Name`
- `salesforce.categories.products_org_sample[].ProductCode`
- `salesforce.categories.products_org_sample[].Description`
- `salesforce.categories.products_org_sample[].IsActive`
- `salesforce.categories.products_org_sample[].Family`
- `salesforce.categories.products_org_sample[].CreatedDate`
- `salesforce.categories.pricebooks_org_sample[].Id`
- `salesforce.categories.pricebooks_org_sample[].Name`
- `salesforce.categories.pricebooks_org_sample[].IsActive`
- `salesforce.categories.pricebooks_org_sample[].IsStandard`
- `salesforce.categories.pricebooks_org_sample[].Description`

