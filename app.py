from flask import Flask, render_template, request
from query import query as query_func  # 你写的查询函数

app = Flask(__name__)


@app.route('/')
def index():
    return render_template('index.html')  # 渲染首页


@app.route('/search')
def search():
    query = request.args.get('q', '')  # 获取查询字符串
    results = query_func(query) if query else []
    return render_template('results.html', query=query, results=results)


# if __name__ == '__main__':
#     app.run(debug=True)
