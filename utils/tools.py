import requests
import tldextract
import yaml
import hashlib
import re

from director import task, config
from urllib.parse import urlparse

import pika
import json
import traceback

import logging

logger = logging.getLogger(__name__)

def basic_publish(queue, message, url_params):
    params = pika.URLParameters(url_params)
    params.socket_timeout = 15
    connection = None
    try:
        connection = pika.BlockingConnection(params)  # Connect to CloudAMQP
        channel = connection.channel()
        channel.basic_publish(exchange='', routing_key=queue,
                              body=json.dumps(message))
    except Exception as e:
        output = traceback.format_exc()
        logger.warning(f"Exception while sending a message to the bot: {output}")
        raise e
    finally:
        if connection:
            connection.close()

def extract_url_info(url):
    uri = urlparse(url)
    return uri.scheme, uri.netloc, uri.path

def extract_domain(url):
    uri = urlparse(url)
    return 'gitee' if tldextract.extract(uri.netloc).domain == 'gitee' else 'github'

def extract_path(url):
    uri = urlparse(url)
    return uri.path

def normalize_url(url):
    uri = urlparse(url)
    return f"{uri.scheme}://{uri.netloc}{uri.path}"

def normalize_key(url):
    uri = urlparse(url)
    domain_name = tldextract.extract(uri.netloc).domain
    return f"{domain_name}{uri.path.replace('/', '-')}".lower()

def hash_string(string):
    h = hashlib.new('sha256')
    h.update(bytes(string, encoding='utf-8'))
    return h.hexdigest()

def url_is_valid(url):
    regex = re.compile(
        r'^(?:http|ftp)s?://' # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' #domain...
        r'localhost|' #localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # ...or ip
        r'(?::\d+)?' # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    return re.match(regex, url) is not None

def gen_project_section(project_data, domain_name, key, url):
    if domain_name == 'gitee':
        project_data[key] = {}
        project_data[key]['git'] = [f"{url}.git"]
        project_data[key][domain_name] = [url]
        project_data[key][f"{domain_name}:pull"] = [url]
        project_data[key][f"{domain_name}2:issue"] = [url]
        project_data[key][f"{domain_name}2:pull"] = [url]
        project_data[key][f"{domain_name}:repo"] = [url]
    elif domain_name == 'github':
        project_data[key] = {}
        project_data[key]['git'] = [f"{url}.git"]
        project_data[key][f"{domain_name}:issue"] = [url]
        project_data[key][f"{domain_name}:pull"] = [url]
        project_data[key][f"{domain_name}2:issue"] = [url]
        project_data[key][f"{domain_name}2:pull"] = [url]
        project_data[key][f"{domain_name}:repo"] = [url]
    return project_data

def gen_project_section_plus(project_data, domain_name, key, url):
    if domain_name == 'gitee':
        project_data[key] = {}
        project_data[key]['git'] = [f"{url}.git"]
        project_data[key][domain_name] = [url]
        project_data[key][f"{domain_name}:pull"] = [url]
        project_data[key][f"{domain_name}2:issue"] = [url]
        project_data[key][f"{domain_name}2:pull"] = [url]
        project_data[key][f"{domain_name}:repo"] = [url]
    elif domain_name == 'github':
        project_data[key] = {}
        project_data[key]['git'] = [f"{url}.git"]
        project_data[key][f"{domain_name}:issue"] = [url]
        project_data[key][f"{domain_name}:pull"] = [url]
        project_data[key][f"{domain_name}2:issue"] = [url]
        project_data[key][f"{domain_name}2:pull"] = [url]
        project_data[key][f"{domain_name}:repo"] = [url]
        project_data[key][f"{domain_name}ql:event"] = [url]
        project_data[key][f"{domain_name}ql:stargazer"] = [url]
        project_data[key][f"{domain_name}ql:fork"] = [url]
    return project_data

def load_yaml_template(url):
    proxies = {
        'http': config.get('GITHUB_PROXY'),
        'https': config.get('GITHUB_PROXY'),
    }
    uri = urlparse(url)
    domain_name = tldextract.extract(uri.netloc).domain
    if domain_name == 'gitee':
        return yaml.safe_load(requests.get(url, allow_redirects=True).text)
    else:
        return yaml.safe_load(requests.get(url, allow_redirects=True, proxies=proxies).text)



def count_repos_group(yaml):
    count, gitee_count, github_count = 0, 0, 0
    for (project_type, project_info) in yaml['resource_types'].items():
        suffix = None
        if project_type == 'software-artifact-repositories' or \
           project_type == 'software-artifact-resources' or \
           project_type == 'software-artifact-projects':
            suffix = 'software-artifact'
        if project_type == 'governance-repositories' or \
           project_type == 'governance-resources' or \
           project_type == 'governance-projects':
            suffix = 'governance'
        if suffix:
            urls = project_info['repo_urls']
            for project_url in urls:
                if not url_is_valid(project_url):
                    continue
                if extract_domain(project_url) == 'gitee':
                    gitee_count += 1
                if extract_domain(project_url) == 'github':
                    github_count += 1
                count += 1
    return count, gitee_count, github_count

def count_repos(yaml):
    return count_repos_group(yaml)[0]
