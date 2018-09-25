from __future__ import absolute_import
from flask import Blueprint, current_app as app, render_template as tmpl, abort, jsonify
from dpark.utils.dag import trans
from dpark.utils.log import get_logger
import json

bp = Blueprint('index', __name__)
logger = get_logger(__name__)


@bp.route('/')
@bp.route('/index/')
def index():
    if hasattr(app.context, "scheduler") and app.context.is_dstream:  # online
        return "dstream not support ui for now."
    return tmpl('dag.html')


@bp.route('/data/')
def data():
    if hasattr(app.context, "scheduler"):  # online
        profs = app.context.scheduler.get_profs()
        dag = trans(profs)
        context = json.dumps(dag, indent=4)
        return context
    else:  # offline
        return app.context


@bp.route('/threads/')
def threads():
    from dpark.utils.frame import get_stacks_of_threads
    res = jsonify(get_stacks_of_threads())
    return res


@bp.route('/stages/')
def stages():
    if hasattr(app.context, "scheduler"):
        return tmpl('jobs.html', stage_infos=app.context.scheduler.idToRunJob)
    else:
        return tmpl('dag.html')


@bp.route('/stages/<stage_id>/')
def stage(stage_id):
    try:
        id_ = int(stage_id)
    except ValueError:
        id_ = 0
        abort(404)
    if id_ not in app.context.scheduler.idToRunJob:
        abort(404)
    return tmpl('stages.html', id=stage_id,
                stage_tuples=app.context.scheduler.
                idToRunJob[id_].get_stage_tuples())
