import os
import logging
from json import dumps

from flask import Flask, g, Response, request
from neo4j import GraphDatabase, basic_auth

app = Flask(__name__, static_url_path = "/static/")

# Try to load database connection info from environment
url = os.getenv("NEO4J_URI", "bolt://localhost:7687")
username = os.getenv("NEO4J_USER", "neo4j2")
password = os.getenv("NEO4J_PASSWORD", "neo4j2")
neo4jVersion = os.getenv("NEO4J_VERSION", "4")
database = os.getenv("NEO4J_DATABASE", "neo4j")
port = os.getenv("PORT", 8080)
user = ""

# Create a database driver instance
driver = GraphDatabase.driver(url, auth = basic_auth(username, password))

# Connect to database only once and store session in current context
def get_db():
    if not hasattr(g, "neo4j_db"):
        if neo4jVersion.startswith("4"):
            g.neo4j_db = driver.session(database = database)
        else:
            g.neo4j_db = driver.session()
    return g.neo4j_db

# Close database connection when application context ends
@app.teardown_appcontext
def close_db(error):
    if hasattr(g, "neo4j_db"):
        g.neo4j_db.close()

def serialize_propietario(propietario):
    return {
        'id': propietario['id'],
        'nombre': propietario['nombre'],
        'mascota': propietario['mascota']
    }

def serialize_publicacion(publicacion):
    return {
        'id': publicacion['id'],
        'mascota': publicacion['mascota'],
        'fotografia': publicacion['fotografia'],
        'like': publicacion.get('like', 0)
    }

def serialize_mascota(mascota):
    return {
        'id': mascota['id'],
        'nombre': mascota['nombre'],
        'especie': mascota['especie'],
    }

@app.route("/")
def get_index():
    return app.send_static_file('index.html')

@app.route("/publicacion/<id>")
def get_publicacion(id):
    db = get_db()
    result = db.read_transaction(lambda tx: tx.run("MATCH (publicacion:publicacion {id:$id}) "
                                                   "OPTIONAL MATCH (mascota)<-[r]-(propietario:propietario) "
                                                   "RETURN publicacion.mascota as mascota,"
                                                   "COLLECT([propietario.nombre, "
                                                   "HEAD(SPLIT(TOLOWER(TYPE(r)), '_')), r.roles]) AS cast "
                                                   "LIMIT 1", {"id": id}).single())

    return Response(dumps({"title": result['title'],
                           "cast": [serialize_publicacion(publicacion)
                                    for publicacion in result['cast']]}),
                    mimetype="application/json")

@app.route("/pulicacion/<id>/vote", methods=["POST"])
def like_publicacion(id):
    db = get_db()
    summary = db.write_transaction(lambda tx: tx.run("MATCH (publicacion:publicacion {id:$id}) "
                                                    "WITH m, (CASE WHEN exists(m.votes) THEN m.votes ELSE 0 END) AS currentVotes "
                                                    "SET m.votes = currentVotes + 1;", {"id": id}).consume())
    updates = summary.counters.properties_set

    db.close()

    return Response(dumps({"updates": updates}), mimetype="application/json")

@app.route("/login" , methods=["POST"])
def get_user(pUser):
    user = pUser
    return app.send_static_file('index.html')

if __name__ == '__main__':
    logging.info('Running on port %d, database is at %s', port, url)
    app.run(port=port)