import os

from flask import Flask, request, make_response, send_file
from db.interface import DBConnection

app = Flask(__name__)

@app.route('/', methods=["GET"])
def getKML():
    zipcode = request.args.get('zipcode',type=str)
    if zipcode is None:
        return "No zipcode provided", 400
    db = DBConnection()

    info = db.search(zipcode)

    if info is None:
        return "No KML found", 404

    _, tail = os.path.split(info[1])
    # print(_,tail)
    res= make_response(send_file(info[1],attachment_filename=tail))

    res.headers['Content-Type'] = 'application/vnd.google-earth.kml+xml'

    return res
