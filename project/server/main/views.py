import redis
from rq import Queue, Connection
from flask import render_template, Blueprint, jsonify, request, current_app

import json

from project.server.main.utils import chunks, to_jsonl
from project.server.main.tasks import create_task_match
from project.server.main.matcher import pre_process_publications, match_all

main_blueprint = Blueprint("main", __name__,)
from project.server.main.logger import get_logger

logger = get_logger(__name__)

MOUNTED_VOLUME = '/upw_data/'


@main_blueprint.route("/", methods=["GET"])
def home():
    return render_template("main/home.html")

@main_blueprint.route("/match_all", methods=["POST"])
def run_task_match_all():

    args = request.get_json(force=True)
    if args.get('preprocess', True):
        pre_process_publications(args)
    # TODO
    # arg to clean output database
    author_keys = json.load(open(f'{MOUNTED_VOLUME}/author_keys.json', 'r'))
    logger.debug(f'There are {len(author_keys)} author_keys')
    author_keys_chunks = list(chunks(lst=author_keys, n=100))
    for chunk in author_keys_chunks:
        with Connection(redis.from_url(current_app.config["REDIS_URL"])):
            q = Queue("person-matcher", default_timeout=2160000)
            task = q.enqueue(match_all, chunk)

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
