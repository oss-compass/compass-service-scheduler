from director import task, config
from datetime import datetime

from . import config_logging
from ..utils import tools

from compass_metrics_model.metrics_model_lab import (
    StarterProjectHealthMetricsModel
)

@task(name="lab_v1.extract", bind=True)
def extract(self, *args, **kwargs):
    payload = kwargs['payload']
    url = payload['project_url']
    level = payload.get('level')
    params = {}
    params['scheme'], params['domain'], params['path'] = tools.extract_url_info(url)
    params['project_url'] = tools.normalize_url(url)
    params['domain_name'] = tools.extract_domain(url)
    params['project_key'] = tools.normalize_key(url)
    params['project_hash'] = tools.hash_string(params['project_url'])
    params['raw'] = bool(payload.get('raw'))
    params['identities_load'] = bool(payload.get('identities_load'))
    params['identities_merge'] = bool(payload.get('identities_merge'))
    params['enrich'] = bool(payload.get('enrich'))
    params['panels'] = bool(payload.get('panels'))
    params['level'] = ('community' if level == 'project' or level == 'community' else 'repo')
    params['debug'] = bool(payload.get('debug'))
    params['metrics_starter_project_health'] = bool(payload.get('metrics_starter_project_health'))
    params['force_refresh_enriched'] = bool(payload.get('force_refresh_enriched'))

    return params

@task(name="lab_v1.metrics.starter_project_health", bind=True)
def initialize(self, *args, **kwargs):
    params = args[0]
    project_key = params['project_key']
    config_logging(params['debug'], params['project_logs_dir'])
    params['metrics_starter_project_health_started_at'] = datetime.now()

    if params.get('metrics_starter_project_health'):
        metrics_cfg = {}
        metrics_cfg['url'] = config.get('ES_URL')
        metrics_cfg['params'] = {
            'issue_index': params['project_issues_index'],
            'pr_index': params['project_pulls_index'],
            'repo_index': params['project_repo_index'],
            'json_file': params['metrics_data_path'],
            'git_index': params['project_git_index'],
            'out_index': f"{config.get('METRICS_OUT_INDEX')}_starter_project_health",
            'from_date': config.get('METRICS_FROM_DATE'),
            'end_date': datetime.now().strftime('%Y-%m-%d'),
            'community': project_key,
            'level': params['level'],
            'contributors_index': params['project_contributors_index'],
            'release_index': params['project_release_index']
        }
        params["metrics_starter_project_health_params"] = metrics_cfg
        model_starter_project_health = StarterProjectHealthMetricsModel(**metrics_cfg['params'])
        model_starter_project_health.metrics_model_metrics(metrics_cfg['url'])
        params['metrics_starter_project_health_finished_at'] = datetime.now()
    else:
        params['metrics_starter_project_health_finished_at'] = 'skipped'
    return params
