import requests
import tldextract
import yaml

from director import task, config
from urllib.parse import urlparse

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
