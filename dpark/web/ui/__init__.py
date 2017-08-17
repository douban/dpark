from __future__ import absolute_import
from flask import Flask
from werkzeug.utils import import_string

blueprints = {
    'index',
}


def create_app(context):
    app = Flask(
        __name__,
        static_folder='static',
        template_folder='templates'
    )
    init_blueprints(app, blueprints)
    app.context = context
    return app


def init_blueprints(app, bps):
    for bp in bps:
        package = __package__ or __name__
        import_name = '%s.views.%s:bp' % (package, bp)
        app.register_blueprint(import_string(import_name))
