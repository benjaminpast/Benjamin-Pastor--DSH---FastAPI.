from fastapi import FastAPI, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from databases import Database

database = Database("sqlite:///fast.db")
app= FastAPI()

@app.on_event("startup")
async def database_connect():
    await database.connect()


@app.on_event("shutdown")
async def database_disconnect():
    await database.disconnect()

#Año con más carreras
@app.get("/Año_más_carreras")
async def fetch_data(id: int):
    consulta = "SELECT year, count(raceId) AS carreras FROM races GROUP BY year ORDER BY carreras DESC LIMIT {}".format(str(id))
    results = await database.fetch_all(query=consulta)
    return  results

#Pilotocon mayor cantidad de primeros puestos
@app.get("/Piloto_con_mas_1")
async def data():
    consulta = "SELECT r.driverId ,surname,forename, count(r.position) FROM results r JOIN drivers d ON r.driverId = d.driverId WHERE Position = 1".format(str(id))
    results = await database.fetch_all(query=consulta)
    return  results
#Nombre del circuito_más_corrido
@app.get("/circuito_más_corrido")
async def fetch_data(id: int):
    consulta = "SELECT c.name, count(raceId) as carreras   FROM races  r join circuits c on c.circuitId = r.circuitId GROUP BY c.name ORDER BY carreras DESC LIMIT 1".format(str(id))
    results = await database.fetch_all(query=consulta)
    return  results
#Piloto con mayor cantidad de puntos en total, cuyo constructor sea de nacionalidad sea American o British
@app.get("/Piloto_mas_puntos")
async def fetch_data(id: int):
    consulta = "SELECT d.forename, d.surname ,r.driverId, sum(r.points) as puntaje_total FROM results r JOIN drivers d on r.driverId = d.driverId JOIN constructors c on r.constructorId = c.constructorId WHERE c.nationality = 'British' or c.nationality = 'American' GROUP BY r.driverId ORDER BY puntaje_total DESC LIMIT {}".format(str(id))
    results = await database.fetch_all(query=consulta)
    return  results

