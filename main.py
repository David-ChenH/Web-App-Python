# Lahman.py

# Convert to/from web native JSON and Python/RDB types.
import json

# Include Flask packages
from flask import Flask
from flask import request
import copy

import BO

# The main program that executes. This call creates an instance of a
# class and the constructor starts the runtime.
app = Flask(__name__,static_folder="static")


def parse_and_print_args():

    fields = None
    in_args = None
    path = request.path
    if request.args is not None:
        in_args = dict(copy.copy(request.args))
        fields = copy.copy(in_args.get('fields', None))
        limit = copy.copy(in_args.get('limit' , None))
        offset = copy.copy(in_args.get('offset', None))
        if fields:
            del(in_args['fields'])
        if limit:
            del(in_args['limit'])
        if offset:
            del(in_args['offset'])

    try:
        if request.data:
            print(request.data)
            body = json.loads(request.data)
        else:
            body = None
    except Exception as e:
        print("Got exception = ", e)
        body = None

    print("Request.args : ", json.dumps(in_args))
    print("body: ",body)
    return in_args, fields, body, limit, offset, path


@app.route('/api/<resource>/<primary_key>', methods=['GET', 'PUT', 'DELETE'])
def get_resource2(resource, primary_key):
    in_args, fields, body, limit, offset, path = parse_and_print_args()
    for (k, v) in in_args.items():
        in_args[k] = "".join(v)

    if request.method == 'GET':
        result = {}
        result["data"] = BO.find_by_primary_key(resource, primary_key, fields)
        if result:
            return json.dumps(result), 200, {'Content-Type': 'application/json; charset=utf-8'}
        else:
            return "NOT FOUND", 404

    elif request.method == 'PUT':
        result = BO.update(resource, primary_key, body)
        return json.dumps(result), 200, {'Content-Type': 'application/json; charset=utf-8'}

    elif request.method == 'DELETE':
        result = BO.delete(resource, primary_key)
        return json.dumps(result), 200, {'Content-Type': 'application/json; charset=utf-8'}

    else:
        return "Method " + request.method + " on resource " + resource + \
               " not implemented!", 501, {"content-type": "text/plain; charset: utf-8"}


@app.route('/api/<resource>', methods=['GET', 'POST'])
def get_resource1(resource):

    in_args, fields, body, limit, offset, path = parse_and_print_args()
    for (k,v) in in_args.items():
        in_args[k] = "".join(v)


    if resource == 'roster':
        result = BO.roster(in_args['teamid'], in_args['yearid'])
        return json.dumps(result), 200, {'Content-Type': 'application/json; charset=utf-8'}

    result = {}
    if request.method == 'GET':
        if limit is None:
            limit = ['10']
        if offset is None:
            offset = ['0']
        result["data"] = BO.find_by_template(resource, in_args, fields, limit, offset)

        current_link = path + "?"
        next_link = path + "?"
        previous_link = path + "?"
        for (k,v) in in_args.items():
            current_link = current_link + k + "=" + v + "&"
            next_link = next_link + k + "=" + v + "&"
            previous_link = previous_link + k + "=" + v + "&"
        if fields is not None:
            current_link = current_link + "fields" + "=" + fields[0] + "&"
            next_link = next_link + "fields" + "=" + fields[0] + "&"
            previous_link = previous_link + "fields" + "=" + fields[0] + "&"
        current_link = current_link + "offset" + "=" + offset[0] + "&" + "limit" + limit[0]
        next_link = next_link + "offset" + "=" + str(int(offset[0])+int(limit[0])) + "&" + "limit" + limit[0]
        previous_link = previous_link + "offset" + "=" + str(int(offset[0])-int(limit[0])) + "&" + "limit" + limit[0]
        current = {}
        current["current"] = current_link
        next = {}
        next["next"]  = next_link
        previous = {}
        previous["previous"] = previous_link
        links = []
        if offset[0] == "0":
            links.append(current)
            links.append(next)
        else:
            links.append(previous)
            links.append(current)
            links.append(next)
        result["links"] = links

        if result:
            return json.dumps(result), 200, {'Content-Type': 'application/json; charset=utf-8'}
        else:
            return "NOT FOUND", 404

    elif request.method == 'POST':
        result = BO.insert(resource, body)
        return json.dumps(result), 200, {'Content-Type': 'application/json; charset=utf-8'}

    else:
        return "Method " + request.method + " on resource " + resource + \
               " not implemented!", 501, {"content-type": "text/plain; charset: utf-8"}


@app.route('/api/<resource>/<primary_key>/<related_resource>', methods=['GET', "POST"])
def get_resoruce3(resource, primary_key, related_resource):
    in_args, fields, body, limit, offset, path = parse_and_print_args()
    for (k, v) in in_args.items():
        in_args[k] = "".join(v)
    key_pair = BO.primary_key_pair(resource, primary_key)

    if request.method == 'GET':
        for (k, v) in key_pair.items():
             in_args[k] = v
        print(in_args)
        result = BO.find_by_template(related_resource, in_args, fields)
        return json.dumps(result), 200, {'Content-Type': 'application/json; charset=utf-8'}

    if request.method == 'POST':
        for (k, v) in key_pair.items():
            body[k]=v
        result = BO.insert(related_resource, body)
        return json.dumps(result), 200, {'Content-Type': 'application/json; charset=utf-8'}


@app.route('/api/teammates/<playerid>', methods=['GET'])
def get_resource4(playerid):
    result = BO.search_teammate(playerid)
    return json.dumps(result), 200, {'Content-Type': 'application/json; charset=utf-8'}


@app.route('/api/people/<playerid>/career_stats')
def get_resource5(playerid):
    result = BO.career_stats(playerid)
    return json.dumps(result), 200, {'Content-Type': 'application/json; charset=utf-8'}

if __name__ == '__main__':
    app.run()




