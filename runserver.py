# runserver.py
import os
import DeviceLoopBackend
from DeviceLoopBackend import create_app

print("Current working directory:", os.getcwd())
print("DeviceLoopBackend loaded from:", DeviceLoopBackend.__file__)

app = create_app()

print("Buyer routes mounted:")
for r in app.url_map.iter_rules():
    if r.rule.startswith("/buyer") or r.rule == "/debug/routes":
        print(" -", r.rule, sorted(list(r.methods or [])))

if __name__ == "__main__":
    app.run(
        host="localhost",
        port=5000,
        debug=True,
        use_reloader=False,
        ssl_context=("certs/localhost+2.pem", "certs/localhost+2-key.pem"),
    )
