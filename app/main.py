from app.server import RedisServer

if __name__ == "__main__":
    socket_server = RedisServer("localhost",6379)
    socket_server.serve()
