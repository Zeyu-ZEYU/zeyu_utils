import pickle
import socket
import sys
import threading
import time

import zeyu_utils.os as zos


class SocketMsger:
    def __init__(self, socket, is_listener=False):
        self.piggyback_data = None
        self.__socket = socket
        self.__is_listener = is_listener
        self.__is_blocking = True
        self.__recv_buffer = b""
        self.__closed = False

    @property
    def socket(self):
        return self.__socket

    @property
    def is_listener(self):
        return self.__is_listener

    @property
    def is_blocking(self):
        return self.__is_blocking

    @property
    def closed(self):
        if getattr(self.__socket, "_closed") is True and self.__closed is False:
            self.__closed = True
        return self.__closed

    def send(self, data):
        if self.__closed or self.__is_listener:
            return
        if isinstance(data, str):
            serialized = 0
            byte_data = data.encode()
        else:
            serialized = 1
            byte_data = pickle.dumps(data, protocol=pickle.HIGHEST_PROTOCOL)
        length = len(byte_data)
        self.__socket.sendall(f"META({length},{serialized})".encode() + byte_data)

    def recv(self, blocking=True):
        if self.__closed or self.__is_listener:
            return
        if blocking:
            if not self.__is_blocking:
                self.__socket.setblocking(True)
                self.__is_blocking = True
        else:
            if self.__is_blocking:
                self.__socket.setblocking(False)
                self.__is_blocking = False
        index = self.__recv_buffer.find(b"META(")
        while index == -1:
            try:
                data = self.__socket.recv(1024)
                if data == b"":
                    self.__closed = True
                    return
                self.__recv_buffer += data
                index = self.__recv_buffer.find(b"META(")
            except BlockingIOError:
                return
        meta_lindex = index + 5
        index = self.__recv_buffer.find(b")", meta_lindex)
        while index == -1:
            try:
                data = self.__socket.recv(1024)
                if data == b"":
                    self.__closed = True
                    return
                self.__recv_buffer += data
                index = self.__recv_buffer.find(b")", meta_lindex)
            except BlockingIOError:
                return
        meta_rindex = index
        meta = self.__recv_buffer[meta_lindex:meta_rindex].split(b",")
        length = int(meta[0])
        deserialized = int(meta[1])
        body_lindex = meta_rindex + 1
        while len(self.__recv_buffer) - body_lindex < length:
            try:
                data = self.__socket.recv(1024)
                if data == b"":
                    self.__closed = True
                    return
                self.__recv_buffer += data
            except BlockingIOError:
                return
        body_rindex = body_lindex + length
        recvd_data = self.__recv_buffer[body_lindex:body_rindex]
        self.__recv_buffer = self.__recv_buffer[body_rindex:]
        if deserialized:
            return pickle.loads(recvd_data)
        else:
            return recvd_data.decode()

    def close(self):
        self.__socket.close()
        self.__closed = True

    @staticmethod
    def tcp_listener(listening_ip, listening_port, backlog=100):
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind((listening_ip, listening_port))
        listener.listen(backlog)
        return SocketMsger(listener, True)

    def accept(self):
        if self.__is_listener:
            conn, address = self.__socket.accept()
            connm = SocketMsger(conn)
            return connm, address

    @staticmethod
    def tcp_connect(ip, port, retry=True):
        sock = None
        while True:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                sock.connect((ip, port))
                return SocketMsger(sock)
            except Exception as e:
                print("NOT CONNECTED:", e, file=sys.stderr)
                if not retry:
                    return
                time.sleep(1)


class RemoteProgramRunner:
    def __init__(self, listening_ip, listening_port):
        self.__listener = SocketMsger.tcp_listener(listening_ip, listening_port)
        self.__thread = threading.Thread(target=self.__run)
        self.__thread.start()

    def __run(self):
        while True:
            connm, _ = self.__listener.accept()
            thread = threading.Thread(target=self.__connm_thread, args=(connm,))
            thread.start()

    def __connm_thread(self, connm):
        cmd = connm.recv()
        if cmd is None:
            return
        thread = threading.Thread(target=zos.run_cmd, args=(cmd,))
        thread.start()

    @staticmethod
    def send_cmd(ip, port, cmd):
        SocketMsger.tcp_connect(ip, port).send(cmd)
