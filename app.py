from flask import Flask
from flask import request, jsonify
import happybase

app = Flask(__name__)
app.config["DEBUG"] = True

port = 49173


@app.route('/db_init', methods=['GET'])
def db_init():
    connection = happybase.Connection('0.0.0.0', port)
    connection.open()

    connection.delete_table('notifications', disable=True)
    connection.create_table(
        'notifications',
        {
            'from_to': dict(),
            'extras': dict()
        }
    )

    connection.close()
    return "OK"


@app.route('/db_conn_test', methods=['GET'])
def db_conn_test():
    connection = happybase.Connection('0.0.0.0', port)
    connection.open()
    x = connection.tables()
    connection.close()
    return jsonify(x)


@app.route('/', methods=['GET'])
def home():
    return "<h1>ATD Projekt - HBASE</h1><p>s15052</p>"


@app.route('/api/notifications/<notification_type>', methods=['GET'])
def get_notification(notification_type):
    connection = happybase.Connection(host='0.0.0.0', port=port)
    connection.open()
    table = connection.table('notifications')
    objects = {}
    filter = "SingleColumnValueFilter ('extras','type',=,'regexstring:^" + \
        notification_type+"$')"
    for key, data in table.scan(

        reverse=True,
        filter=filter
    ):
        objects[key] = data
    connection.close()
    return jsonify(objects)


@app.route('/api/notifications/comment/<key>', methods=['POST'])
def comment(key):
    if request.method == "POST":
        request_data = request.get_json()
        connection = happybase.Connection(host='0.0.0.0', port=port)
        connection.open()
        table = connection.table('notifications')
        table.put(str.encode(key), {
            b'from_to:from_user': str.encode(request_data['from_user']),
            b'from_to:for_user': str.encode(request_data['for_user']),
            b'extras:commented_on': str.encode(request_data['commented_on']),
            b'extras:text': str.encode(request_data['text']),
            b'extras:url': str.encode(request_data['url']),
            b'extras:type': b'comment'
        })
        connection.close()
        return request_data


@app.route('/api/notifications/like/<key>', methods=['POST'])
def like(key):
    if request.method == "POST":
        request_data = request.get_json()
        connection = happybase.Connection(host='0.0.0.0', port=port)
        connection.open()
        table = connection.table('notifications')
        table.put(str.encode(key), {
            b'from_to:from_user': str.encode(request_data['from_user']),
            b'from_to:for_user': str.encode(request_data['for_user']),
            b'extras:liked': str.encode(request_data['liked']),
            b'extras:url': str.encode(request_data['url']),
            b'extras:type': b'like'
        })
        connection.close()
        return request_data


@app.route('/api/notifications/friendRequest/<key>', methods=['POST'])
def friend_request(key):
    if request.method == "POST":
        request_data = request.get_json()
        connection = happybase.Connection(host='0.0.0.0', port=port)
        connection.open()
        table = connection.table('notifications')
        table.put(str.encode(key), {
            b'from_to:from_user': str.encode(request_data['from_user']),
            b'from_to:for_user': str.encode(request_data['for_user']),
            b'extras:type': b'friendRequest'
        })
        connection.close()
        return request_data


@app.route('/api/notifications/pmMessage/<key>', methods=['POST'])
def pm_message(key):
    if request.method == "POST":
        request_data = request.get_json()
        connection = happybase.Connection(host='0.0.0.0', port=port)
        connection.open()
        table = connection.table('notifications')
        table.put(str.encode(key), {
            b'from_to:from_user': str.encode(request_data['from_user']),
            b'from_to:for_user': str.encode(request_data['for_user']),
            b'extras:commented_on': str.encode(request_data['commented_on']),
            b'extras:text': str.encode(request_data['text']),
            b'extras:type': b'pmMessage'
        })
        connection.close()
        return request_data


app.run()
