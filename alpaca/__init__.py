from flask import Blueprint

alpaca_bp = Blueprint('alpaca', __name__,
                      template_folder='../templates/alpaca',
                      url_prefix='/alpaca')
