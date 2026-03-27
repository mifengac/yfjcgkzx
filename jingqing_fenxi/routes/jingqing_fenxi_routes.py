from flask import Blueprint, render_template

jingqing_fenxi_bp = Blueprint('jingqing_fenxi', __name__, template_folder='../templates', static_folder='../static')

@jingqing_fenxi_bp.route('/', methods=['GET'])
def index():
    return render_template("jingqing_fenxi_index.html")


from jingqing_fenxi.routes import analysis_tab_routes  # noqa: E402,F401
from jingqing_fenxi.routes import rental_case_routes  # noqa: E402,F401
from jingqing_fenxi.routes import wander_case_routes  # noqa: E402,F401
