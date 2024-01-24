from director import task, config

from compass_contributor.bot import BotService
from compass_contributor.organization import OrganizationService
from compass_contributor.contributor_org import ContributorOrgService

import os
import logging


logger = logging.getLogger(__name__)


@task(name="schedu_v1.update_contributor_org", autoretry_for=(Exception,), retry_kwargs={'max_retries': 3}, acks_late=True)
def update_contributor_org(*args, **kwargs):
    elastic_url = config.get('ES_URL')
    os.environ["HTTPS_PROXY"] = config.get('GITHUB_PROXY')
    contributor = ContributorOrgService(elastic_url, 'contributor_org', 'github')
    contributor.save_by_cncf_gitdm_url()
    logger.info(f"finish init contributor org by cncf gitdm")

@task(name="schedu_v1.update_orgs", autoretry_for=(Exception,), retry_kwargs={'max_retries': 3}, acks_late=True)
def update_orgs(*args, **kwargs):
    elastic_url = config.get('ES_URL')
    os.environ["HTTPS_PROXY"] = config.get('GITHUB_PROXY')
    organization = OrganizationService(elastic_url, 'organizations')
    organization.save_by_config_file()
    logger.info(f"finish init organization by config")

@task(name="schedu_v1.update_bots", autoretry_for=(Exception,), retry_kwargs={'max_retries': 3}, acks_late=True)
def update_bots(*args, **kwargs):
    elastic_url = config.get('ES_URL')
    os.environ["HTTPS_PROXY"] = config.get('GITHUB_PROXY')
    bot = BotService(elastic_url, 'bots')
    bot.save_by_config_file()
    logger.info(f"finish init organization by config")
