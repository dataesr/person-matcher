import redis
from rq import Queue, Connection
from flask import render_template, Blueprint, jsonify, request, current_app

import json
import pymongo

from project.server.main.utils import chunks, to_jsonl
from project.server.main.tasks import create_task_match
from project.server.main.matcher import pre_process_publications, match_all
from project.server.main.scanr import export_scanr, upload_sword
from project.server.main.scanr2 import export_scanr2
from project.server.main.projects import load_projects
from project.server.main.patents import load_patents
from project.server.main.organisations import load_orga
from project.server.main.geo import load_geo

main_blueprint = Blueprint("main", __name__,)
from project.server.main.logger import get_logger

logger = get_logger(__name__)

MOUNTED_VOLUME = '/upw_data/'


@main_blueprint.route("/", methods=["GET"])
def home():
    return render_template("main/home.html")

@main_blueprint.route("/scanr_other", methods=["POST"])
def run_task_scanr_other():
    args = request.get_json(force=True)
    if args.get('projects'):
        with Connection(redis.from_url(current_app.config["REDIS_URL"])):
            q = Queue("person-matcher", default_timeout=21600000)
            task = q.enqueue(load_projects, args)
    if args.get('patents'):
        with Connection(redis.from_url(current_app.config["REDIS_URL"])):
            q = Queue("person-matcher", default_timeout=21600000)
            task = q.enqueue(load_patents, args)
    if args.get('orga'):
        with Connection(redis.from_url(current_app.config["REDIS_URL"])):
            q = Queue("person-matcher", default_timeout=21600000)
            task = q.enqueue(load_orga, args)
    if args.get('geo'):
        with Connection(redis.from_url(current_app.config["REDIS_URL"])):
            q = Queue("person-matcher", default_timeout=21600000)
            task = q.enqueue(load_geo, args)
    response_object = {
        "status": "success",
        "data": {
            "task_id": task.get_id()
        }
    }
    return jsonify(response_object), 202

@main_blueprint.route("/scanr", methods=["POST"])
def run_task_scanr():
    args = request.get_json(force=True)
    denormalized = args.get('denormalized', False)
    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue("person-matcher", default_timeout=21600000)
        if denormalized:
            task = q.enqueue(export_scanr2, args)
        else:
            task = q.enqueue(export_scanr, args)
    response_object = {
        "status": "success",
        "data": {
            "task_id": task.get_id()
        }
    }
    return jsonify(response_object), 202

@main_blueprint.route('/upload_sword', methods=['POST'])
def run_task_upload_sword():
    args = request.get_json(force=True)
    with Connection(redis.from_url(current_app.config['REDIS_URL'])):
        q = Queue(name='person-matcher', default_timeout=21600000)
        task = q.enqueue(upload_sword, args)
    response_object = {'status': 'success', 'data': {'task_id': task.get_id()}}
    return jsonify(response_object), 202

@main_blueprint.route("/match_all", methods=["POST"])
def run_task_match_all():

    args = request.get_json(force=True)
    if args.get('preprocess', True):
        pre_process_publications(args)
    if args.get('reset_db', True):
        myclient = pymongo.MongoClient('mongodb://mongo:27017/')
        mydb = myclient['scanr']
        logger.debug('dropping collection person_matcher_output')
        mydb['person_matcher_output'].drop()
        myclient.close()

    author_keys = json.load(open(f'{MOUNTED_VOLUME}/author_keys.json', 'r'))
    logger.debug(f'There are {len(author_keys)} author_keys')
    author_keys_chunks = list(chunks(lst=author_keys, n=100))
    harvest_sudoc = args.get('harvest_sudoc', False)
    nb_keys = 0
    for chunk in author_keys_chunks:
        with Connection(redis.from_url(current_app.config["REDIS_URL"])):
            q = Queue("person-matcher", default_timeout=2160000)
            task = q.enqueue(match_all, chunk, harvest_sudoc)
            nb_keys += len(chunk)
            logger.debug(f'{nb_keys} keys sent')

    response_object = {
        "status": "success",
        "data": {
            "task_id": task.get_id()
        }
    }
    return jsonify(response_object), 202



@main_blueprint.route("/match2", methods=["POST"])
def run_task_match2():
    args = request.get_json(force=True)
    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue("person-matcher", default_timeout=21600000)
        task = q.enqueue(create_task_match, args)
    response_object = {
        "status": "success",
        "data": {
            "task_id": task.get_id()
        }
    }
    return jsonify(response_object), 202


@main_blueprint.route("/match", methods=["POST"])
def run_task_match():
    args = request.get_json(force=True)
    #with Connection(redis.from_url(current_app.config["REDIS_URL"])):
    #    q = Queue("person-matcher", default_timeout=216000)
    #    task = q.enqueue(create_task_match, args)
    #response_object = {
    #    "status": "success",
    #    "data": {
    #        "task_id": task.get_id()
    #    }
    #}
    response_object = create_task_match(args)
    return jsonify(response_object), 202

@main_blueprint.route("/tasks/<task_id>", methods=["GET"])
def get_status(task_id):
    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue("person-matcher")
        task = q.fetch_job(task_id)
    if task:
        response_object = {
            "status": "success",
            "data": {
                "task_id": task.get_id(),
                "task_status": task.get_status(),
                "task_result": task.result,
            },
        }
    else:
        response_object = {"status": "error"}
    return jsonify(response_object)
