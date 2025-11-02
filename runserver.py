# runserver.py
from DeviceLoopBackend import create_app
import os

app = create_app()

if __name__ == "__main__":
    app.run(
        host="localhost",
        port=5000,
        debug=True,
        use_reloader=False,
        ssl_context=("certs/localhost+2.pem", "certs/localhost+2-key.pem"),
    )
