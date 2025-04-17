#!/usr/bin/env python3
from flask import Flask
import os
from app.config import Config

def create_app(config_class=Config):
    app = Flask(__name__)
    app.config.from_object(config_class)
    
    # Register blueprints
    from app.routes import main_bp, examples_bp, cluster_bp
    app.register_blueprint(main_bp)
    app.register_blueprint(examples_bp, url_prefix='/examples')
    app.register_blueprint(cluster_bp, url_prefix='/cluster')
    
    return app

if __name__ == '__main__':
    app = create_app()
    app.run(host='0.0.0.0', port=5000, debug=True)