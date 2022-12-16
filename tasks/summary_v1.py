from director import task, config
from datetime import datetime
import logging

from compass_metrics_model.metrics_model_summary import (
   ActivityMetricsSummary,
   CommunitySupportMetricsSummary,
   CodeQualityGuaranteeMetricsSummary,
   OrganizationsActivityMetricsSummary
)

# #Summary Example:
# {
#     "metrics_activity_summary":true,
#     "metrics_community_summary":true,
#     "metrics_codequality_summary":true,
#     "metrics_group_activity_summary":true,
#     "from_date": "2000-01-01",
#     "end_date": "2022-12-25"
# }

@task(name="summary_v1.initialize", bind=True)
def initialize(self, *args, **kwargs):
    payload = kwargs['payload']
    params = {}
    params['metrics_activity_summary'] = bool(payload.get('metrics_activity_summary'))
    params['metrics_community_summary'] = bool(payload.get('metrics_community_summary'))
    params['metrics_codequality_summary'] = bool(payload.get('metrics_codequality_summary'))
    params['metrics_group_activity_summary'] = bool(payload.get('metrics_group_activity_summary'))
    params['from_date'] = payload.get('from_date') or config.get('METRICS_FROM_DATE')
    params['end_date'] = payload.get('end_date') or datetime.now().strftime('%Y-%m-%d')
    return params

@task(name="summary_v1.sum.activtiy", acks_late=True, autoretry_for=(Exception,), retry_kwargs={'max_retries': 3})
def metrics_activity_summary(*args, **kwargs):
    params = args[0]
    params['metrics_activity_summary_started_at'] = datetime.now()
    if params.get('metrics_activity_summary'):
        activity_summary = ActivityMetricsSummary(
            f"{config.get('METRICS_OUT_INDEX')}_activity",
            'Activity',
            params['from_date'],
            params['end_date'],
            f"{config.get('METRICS_OUT_INDEX')}_activity_summary"
        )
        elastic_url = config.get('ES_URL')
        activity_summary.metrics_model_summary(elastic_url)
        params['metrics_activity_summary_finished_at'] = datetime.now()
    else:
        params['metrics_activity_summary_finished_at'] = 'skipped'
    return params

@task(name="summary_v1.sum.community", acks_late=True, autoretry_for=(Exception,), retry_kwargs={'max_retries': 3})
def metrics_community_summary(*args, **kwargs):
    params = args[0]
    params['metrics_community_summary_started_at'] = datetime.now()
    if params.get('metrics_community_summary'):
        community_summary = CommunitySupportMetricsSummary(
            f"{config.get('METRICS_OUT_INDEX')}_community",
            'Community Support and Service',
            params['from_date'],
            params['end_date'],
            f"{config.get('METRICS_OUT_INDEX')}_community_summary"
        )
        elastic_url = config.get('ES_URL')
        community_summary.metrics_model_summary(elastic_url)
        params['metrics_community_summary_finished_at'] = datetime.now()
    else:
        params['metrics_community_summary_finished_at'] = 'skipped'
    return params

@task(name="summary_v1.sum.codequality", acks_late=True, autoretry_for=(Exception,), retry_kwargs={'max_retries': 3})
def metrics_codequality_summary(*args, **kwargs):
    params = args[0]
    params['metrics_codequality_summary_started_at'] = datetime.now()
    if params.get('metrics_codequality_summary'):
        codequality_summary = CodeQualityGuaranteeMetricsSummary(
            f"{config.get('METRICS_OUT_INDEX')}_codequality",
            'Code_Quality_Guarantee',
            params['from_date'],
            params['end_date'],
            f"{config.get('METRICS_OUT_INDEX')}_codequality_summary"
        )
        elastic_url = config.get('ES_URL')
        codequality_summary.metrics_model_summary(elastic_url)
        params['metrics_codequality_summary_finished_at'] = datetime.now()
    else:
        params['metrics_codequality_summary_finished_at'] = 'skipped'
    return params

@task(name="summary_v1.sum.group_activity_summary", acks_late=True, autoretry_for=(Exception,), retry_kwargs={'max_retries': 3})
def metrics_group_activity_summary(*args, **kwargs):
    params = args[0]
    params['metrics_group_activity_summary_started_at'] = datetime.now()
    if params.get('metrics_group_activity_summary'):
        organizations_activity_summary = OrganizationsActivityMetricsSummary(
            f"{config.get('METRICS_OUT_INDEX')}_group_activity",
            'Code_Quality_Guarantee',
            params['from_date'],
            params['end_date'],
            f"{config.get('METRICS_OUT_INDEX')}_group_activity_summary"
        )
        elastic_url = config.get('ES_URL')
        organizations_activity_summary.metrics_model_summary(elastic_url)
        params['metrics_group_activity_summary_finished_at'] = datetime.now()
    else:
        params['metrics_group_activity_summary_finished_at'] = 'skipped'
    return params
