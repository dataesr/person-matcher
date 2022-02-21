import time
import datetime
import os
import requests
from project.server.main.matcher import match

from project.server.main.logger import get_logger

logger = get_logger(__name__)

def create_task_matcher(arg):
    publications = args.get('publications', [])
    first_name = args.get('first_name')
    last_name = args.get('last_name')
    match(publications=publications, first_name=first_name, last_name=last_name)


