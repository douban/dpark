from flask import Blueprint, current_app as app, render_template as tmpl, abort

bp = Blueprint('index', __name__)


@bp.route('/')
@bp.route('/index/')
@bp.route('/stages/')
def stages():
    return tmpl('jobs.html', stage_infos=app.context.scheduler.idToRunJob)


@bp.route('/stages/<stage_id>/')
def stage(stage_id):
    try:
        id = int(stage_id)
    except:
        abort(404)
    if id not in app.context.scheduler.idToRunJob:
        abort(404)
    return tmpl('stages.html', id=stage_id,
                stage_tuples=app.context.scheduler.
                idToRunJob[id].get_stage_tuples())
