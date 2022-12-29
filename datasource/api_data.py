from flask import Flask, jsonify
app = Flask(__name__)


@app.route('/')
def hello_world():
    result = """{"email": "barry.xu@163.com", "job": "engineer"}\n{"email": "dandan@qq.com", "job": "hr"}\n{"email": "focus@qq.com", "job": "analyst"}\n{"email": "pony@qq.com", "job": "sales"}"""
    return result


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5016, debug=True)
