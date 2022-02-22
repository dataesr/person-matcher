import time
import datetime
import os
import requests
from project.server.main.matcher import match

from project.server.main.logger import get_logger

logger = get_logger(__name__)

def create_task_match(arg):
    publications = args.get('publications', [])
    author_key = args.get('author_key')
    return match(publications=publications, author_key=author_key)


