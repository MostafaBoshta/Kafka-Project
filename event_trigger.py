from server import socketio  

def trigger_event():
    socketio.emit('refresh', {})