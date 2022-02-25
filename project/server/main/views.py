import redis
from rq import Queue, Connection
from flask import render_template, Blueprint, jsonify, request, current_app

from project.server.main.tasks import create_task_match
from project.server.main.matcher import match_all

main_blueprint = Blueprint("main", __name__,)
from project.server.main.logger import get_logger

logger = get_logger(__name__)


@main_blueprint.route("/", methods=["GET"])
def home():
    return render_template("main/home.html")

@main_blueprint.route("/match_all", methods=["POST"])
def run_task_match_all():
    args = request.get_json(force=True)
    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue("person-matcher", default_timeout=216000)
        task = q.enqueue(match_all_publications, args)
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
        q = Queue("person-matcher", default_timeout=216000)
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
