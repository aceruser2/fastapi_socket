from fastapi import FastAPI, WebSocket
from fastapi.responses import HTMLResponse
from fastapi_socketio import SocketManager
from aiokafka import AIOKafkaProducer,AIOKafkaConsumer,TopicPartition
from aiokafka.errors import KafkaError
import asyncio
import time
import uuid
import logging
import sys
logger = logging.getLogger('kafka')
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.ERROR)

app = FastAPI()
sio = SocketManager(app=app)

loop = asyncio.get_event_loop()
aioproducer = AIOKafkaProducer(loop = loop,client_id="fastapi_socket_producer",bootstrap_servers='localhost:9092')
aioconsumer = AIOKafkaConsumer(loop = loop,client_id="fastapi_socket_consumer",bootstrap_servers='localhost:9092',auto_offset_reset="latest",group_id="fastapi_socket_consumer")
TOPIC_NAME = "test2"
@app.on_event("startup")
async def startup_event():
    await aioproducer.start()
    await aioconsumer.start()

@app.on_event("shutdown")
async def shutdown_event():
    await aioproducer.stop()
    await aioconsumer.stop()



html="""
<html>

<head>
    <script src="https://ajax.googleapis.com/ajax/libs/jquery/3.4.1/jquery.min.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/socket.io/3.1.1/socket.io.js"
        integrity="sha512-oFOCo2/3DtjrJG4N27BjSLQWoiBv171sK6a+JiWjp/7agxC2nCUP358AqzxkBUb5jX8g6CYLPdSKQTbC0weCwA=="
        crossorigin="anonymous"></script>
    <script type="text/javascript" charset="utf-8">
        $(document).ready(function () {
            var socket = io('ws://0.0.0.0:8000/kafka', {path: "/ws/socket.io/"});


            socket.on('kafkaconsumer', function (msg) {
                $('#consumer').append('<br>' + $('<div/>').text(msg.data).html());
            });

            socket.on('kafkaproducer', function (msg) {
                $('#producer').append('<br>' + $('<div/>').text(msg.data).html());
            });

            socket.on('logs', function (msg) {
                $('#log').append('<br>' + $('<div/>').text(msg.data).html());
            });

            $('form#emit').submit(function (event) {
                socket.emit('kafkaproducer', $('#emit_data').val());
                return false;
            });
        });
    </script>
</head>

<body>
    <form id="emit" method="POST" action='#'>
        <input type="text" name="emit_data" id="emit_data" placeholder="Message">
        <input type="submit" value="Echo">
    </form>
    <h2>Logs</h2>
    <div id="log"></div>
    <h2>Producer</h2>
    <div id="producer"></div>
    <h2>Consumer</h2>
    <div id="consumer"></div>
</body>

</html>



"""

@app.get("/")
async def get():
    return HTMLResponse(html)




@app.sio.on('connect', namespace='/kafka')
async def test_connect(sid,data):
    await sio.emit('logs', {'data': 'Connection established'},namespace='/kafka')

@app.sio.on("kafkaconsumer", namespace="/kafka")
async def kafkaconsumer_endpoint(sid,data):
   

    try:
        tp=TopicPartition(TOPIC_NAME, 0)
        aioconsumer.assign([tp])    
        async for message in aioconsumer:
            if not message:
                time.sleep(100)
            print(message)
            print(type(message))
            await sio.emit('kafkaconsumer',{'data': message.value.decode('utf-8')},namespace='/kafka')
        print(data)
    except KafkaError as e:
        print(e)


@app.sio.on("kafkaproducer",namespace="/kafka")
async def kafkaproduce_endpoint(sid,data):
    try:
        print(data)
        await aioproducer.send_and_wait(TOPIC_NAME,value=bytes(
            str(data), encoding='utf-8'), key=bytes(str(uuid.uuid4()), encoding='utf-8'))
        
        await sio.emit("kafkaproducer",{'data': data},namespace='/kafka')
        await kafkaconsumer_endpoint(sid,data)
    except KafkaError as e:
        print(e)


if __name__ == '__main__':
    import sys

    logging.basicConfig(
        level=logging.DEBUG,
        stream=sys.stdout
    )

    import uvicorn

    uvicorn.run("run:app",host='0.0.0.0',port=8000 ,reload =True,debug=True)