import os
from app import app, scheduler


if __name__ == "__main__":
    scheduler.api_enabled = True
    scheduler.init_app(app)
    scheduler.start()

    app.run(host='0.0.0.0', port=os.environ.get("FLASK_SERVER_PORT"), debug=True)