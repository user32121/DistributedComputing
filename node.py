import os
import socket
import sys
import typing
import time
import threading
import subprocess
import platform
import uuid



#constants
PORT = 8111
HANDSHAKEBYTES = bytes([32, 51, 70])
MAXTIMEOUT = 10
#packet types
TYPE_INVALID = 0
TYPE_HANDSHAKE = 1
TYPE_COMMAND = 2
TYPE_RESPONSE = 3
TYPE_DATA = 4
#commands
COMMAND_PING = 0
COMMAND_PONG = 1
COMMAND_EXIT = 5
COMMAND_GETTASK = 10
COMMAND_GETSUBTASK = 11
COMMAND_SUBMITSUBTASK = 12
COMMAND_ISSUBTASKDONE = 13
COMMAND_SUBMITSUBTASKOUTPUT = 14
#responses
RESPONSE_NODE = 83
RESPONSE_CLIENT = 98
RESPONSE_OK = 0
RESPONSE_DONE = 1
RESPONSE_NONEWTASKS = 11
RESPONSE_DOESNOTHAVEFILE = 12
RESPONSE_NONEWSUBTASKS = 13
RESPONSE_NOTENOUGHSPACE = 14
RESPONSE_NONEWRESULTS = 15
RESPONSE_SENDAUUID = 16
RESPONSE_NOAUUID = 17

NODEFOLDER = "nodeFiles"



class SocketIsClosedException(Exception):
    pass  #dummy class for exiting when socket is closed

def _receiveBytes(connection:socket.socket, numBytes):
    data = bytes()
    while len(data) < numBytes:
        newData = connection.recv(numBytes - len(data))
        if(len(newData) == 0):
            raise (SocketIsClosedException())
        data += newData
    return data

#returns False if failed
#data will contain undefined data
def receive(connection:socket.socket) -> typing.Tuple[int, bytes]:
    try:
        lenghtAsBytes = _receiveBytes(connection, 4)
        length = int.from_bytes(lenghtAsBytes, "big")
        packetTypeAsBytes = _receiveBytes(connection, 4)
        packetType = int.from_bytes(packetTypeAsBytes, "big")
        data = _receiveBytes(connection, length)
        return (packetType, data)
    except SocketIsClosedException:
        print(str(connection.getpeername())+": socket closed")
        return (TYPE_INVALID, 2)
    except ConnectionResetError as e:
        print(e)
        return (TYPE_INVALID, 3)

#returns False if failed
def send(connection:socket.socket, packetType:int, data:typing.Union[bytes, int]) -> bool:
    try:
        if(type(data) == int):
            data = data.to_bytes(4, "big")
        lenghtAsBytes = len(data).to_bytes(4, "big")
        connection.sendall(lenghtAsBytes)
        packeTypeAsBytes = packetType.to_bytes(4, "big")
        connection.sendall(packeTypeAsBytes)
        connection.sendall(data)
        return True
    except socket.timeout:
        return False

socketMutex = threading.Lock()
connectionClosed = False

def regularPing(connection:socket.socket):
    global connectionClosed
    while not connectionClosed:
        socketMutex.acquire()
        print("ping...", end="", flush=True)
        send(connection, TYPE_COMMAND, COMMAND_PING)
        pType, data = receive(connection)
        if(pType == TYPE_INVALID):
            print("server connection closed")
            connectionClosed = True
            socketMutex.release()
            return
        if(pType != TYPE_COMMAND or int.from_bytes(data, "big") != COMMAND_PONG): print("server did not pong ("+str(pType)+": "+str(int.from_bytes(data, "big"))+")")
        print("pong")
        socketMutex.release()
        time.sleep(MAXTIMEOUT / 2)




targetAddress = input("server ip address: ")
connection = socket.create_connection((targetAddress, PORT))
connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
print("connected to "+str(connection.getpeername())+" as "+str(connection.getsockname()))
#handshake
send(connection, TYPE_HANDSHAKE, HANDSHAKEBYTES)
print("sent handshake")
pType, data = receive(connection)
assert pType == TYPE_RESPONSE, "server sent invalid response"
assert int.from_bytes(data, "big") == RESPONSE_OK, "server did not send OK"
print("server ok")
#identify as node
send(connection, TYPE_RESPONSE, RESPONSE_NODE)
print("identified as node")

pingThread = threading.Thread(None, regularPing, None, [connection])
pingThread.start()

#start processing
taskUUID = None
taskUUIDBytes = None
algoUUID = None
hasAltProcessorFile = False
try:
    while not connectionClosed:
        time.sleep(1)
        try:
            socketMutex.acquire()
            print("getting task")
            send(connection, TYPE_COMMAND, COMMAND_GETTASK)
            pType, data = receive(connection)
            assert pType == TYPE_RESPONSE, "server sent invalid response to get task"
            response = int.from_bytes(data, "big")
            if(response == RESPONSE_NONEWTASKS):
                print("no new tasks")
                time.sleep(MAXTIMEOUT/2)
                continue
            elif(response == RESPONSE_OK):
                #task uuid
                pType, data = receive(connection)
                assert pType == TYPE_DATA, "server did not send task uuid"
                taskUUIDBytes = data
                taskUUID = uuid.UUID(bytes=data)
                pType, data = receive(connection)
                assert pType == TYPE_RESPONSE, "server did not send response"
                response = int.from_bytes(data, "big")
                if(response == RESPONSE_SENDAUUID):
                    pType, data = receive(connection)
                    assert pType == TYPE_DATA, "server did not send auuid"
                    algoUUID = uuid.UUID(bytes=data)
                    print("auuid: "+str(algoUUID))
                elif(response == RESPONSE_NOAUUID):
                    algoUUID = None
                    print("no auuid")
                else:
                    raise AssertionError("server did not send response about auuid")

                #generate folders and file
                if(not os.path.isdir(NODEFOLDER)):
                    os.mkdir(NODEFOLDER)
                processorFile = str(taskUUID)+".py"
                processorFilePath = os.path.join(NODEFOLDER, processorFile)
                altProcessorFile = str(algoUUID)
                altProcessorFilePath = os.path.join(NODEFOLDER, altProcessorFile)
                if(os.path.isfile(processorFilePath)):
                    print("has python processor file")
                else:
                    print("does not have python processor file")
                if(os.path.isfile(altProcessorFilePath)):
                    print("has alt processor file")
                    hasAltProcessorFile = True
                else:
                    print("does not have alt processor file")
                    hasAltProcessorFile = False

                if(not os.path.isfile(processorFilePath) and not os.path.isfile(altProcessorFile)):
                    #request file and write
                    send(connection, TYPE_RESPONSE, RESPONSE_DOESNOTHAVEFILE)
                    pType, data = receive(connection)
                    assert pType == TYPE_DATA, "server did not send file"
                    f = open(processorFilePath, "w")
                    f.write(data.decode())
                    f.close()
                    print("received file")
                else:
                    send(connection, TYPE_RESPONSE, RESPONSE_OK)
                print("ready to process subtasks for task "+str(taskUUID))
                print()
            else:
                raise AssertionError("server sent unknown response to get task")
        except AssertionError as e:
            print(e)
        finally:
            socketMutex.release()
        if(taskUUID == None):
            print("no task uuid")
            continue
        
        while(True):
            time.sleep(1)
            inputData = None
            #get subtask data
            try:
                socketMutex.acquire()
                print("getting subtask")
                send(connection, TYPE_COMMAND, COMMAND_GETSUBTASK)
                send(connection, TYPE_DATA, taskUUIDBytes)
                pType, data = receive(connection)
                assert pType == TYPE_RESPONSE, "server sent invalid response to get subtask"
                response = int.from_bytes(data, "big")
                if(response == RESPONSE_NONEWSUBTASKS):
                    print("no new subtasks")
                    break
                elif(response == RESPONSE_OK):
                    pType, data = receive(connection)
                    assert pType == TYPE_DATA, "server did not send uuid"
                    subtaskUUIDBytes = data
                    subtaskUUID = uuid.UUID(bytes=subtaskUUIDBytes)
                    pType, data = receive(connection)
                    assert pType == TYPE_DATA, "server did not send input data"
                    inputData = data
                    print("acquired input data for subtask "+str(subtaskUUID))
                else:
                    raise AssertionError("server sent unknown response to get subtask")
            except AssertionError as e:
                print(e)
            finally:
                socketMutex.release()

            #process data
            if(inputData == None):
                continue
            inputDataAsStr = inputData.decode()
            inputFilePath = os.path.join(NODEFOLDER, "in.txt")
            f = open(inputFilePath, "w")
            f.write(inputDataAsStr)
            f.close()
            errorFilePath = os.path.join(NODEFOLDER, "error.txt")
            errorFile = open(errorFilePath, "w")

            print("processing data")
            errorOccurred = False
            if(hasAltProcessorFile):
                if(subprocess.call("cd \""+NODEFOLDER+"\" && \"./"+altProcessorFile+"\"", shell=True, stderr=errorFile) != 0):
                    errorOccurred = True
            else:
                if(platform.system() == "Windows"):
                    if(subprocess.call("cd \""+NODEFOLDER+"\" && python \""+processorFile+"\"", shell=True, stderr=errorFile) != 0):
                        errorOccurred = True
                elif(platform.system() == "Linux" or platform.system() == "Darwin"):
                    if(subprocess.call("cd \""+NODEFOLDER+"\" && python3 \""+processorFile+"\"", shell=True, stderr=errorFile) != 0):
                        errorOccurred = True
                else:
                    raise AssertionError("unkonwn platform, unsure whether to use python or python3")
            print("done")

            #send output
            try:
                socketMutex.acquire()
                print("submitting results of subtask "+str(subtaskUUID))
                send(connection, TYPE_COMMAND, COMMAND_SUBMITSUBTASKOUTPUT)
                send(connection, TYPE_DATA, subtaskUUIDBytes)
                outputFilePath = os.path.join(NODEFOLDER, "out.txt")
                try:
                    f = open(outputFilePath, "r")
                    outputData = f.read()
                    f.close()
                    outputData = outputData.encode()
                except FileNotFoundError:
                    outputData = "out.txt file not found".encode()
                #also send errors
                if(errorOccurred):
                    f = open(errorFilePath, "r")
                    errorData = f.read()
                    f.close()
                    outputData += errorData.encode()
                send(connection, TYPE_DATA, outputData)
            except AssertionError as e:
                print(e)
            finally:
                print("done")
                socketMutex.release()

        if(socketMutex.locked()):
            socketMutex.release()
except KeyboardInterrupt:
    connectionClosed = True
    socketMutex.acquire()
    send(connection, TYPE_COMMAND, COMMAND_EXIT)

connection.close()
