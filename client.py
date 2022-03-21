import socket
import typing
import time
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
RESPONSE_NONEWTASKS = 1
RESPONSE_DOESNOTHAVEFILE = 2
RESPONSE_NONEWSUBTASKS = 3
RESPONSE_NOTENOUGHSPACE = 4
RESPONSE_NONEWRESULTS = 5



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



def runClient(addr: str, processorFile: str, inputData: typing.Iterable[str]) -> typing.List[str]:
    connection = socket.create_connection((addr, PORT))
    connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    print("connected to "+str(connection.getpeername())+" as "+str(connection.getsockname()))
    #handshake
    send(connection, TYPE_HANDSHAKE, HANDSHAKEBYTES)
    print("sent handshake")
    pType, data = receive(connection)
    assert pType == TYPE_RESPONSE, "server sent invalid response"
    assert int.from_bytes(data, "big") == RESPONSE_OK, "server did not send OK"
    print("server ok")
    #identify as client
    send(connection, TYPE_RESPONSE, RESPONSE_CLIENT)
    print("identified as client")

    #send preliminary data
    print("sending processor file")
    f = open(processorFile, "r")
    send(connection, TYPE_DATA, f.read().encode())
    f.close()
    print("file sent")

    #send requests
    results : "dict[str, str]" = dict()
    pendingSubtasks : "dict[bytes, str]" = dict()
    nextSubtaskInput : str = None
    print("starting processing")
    while True:
        #ping
        print("ping...", end="", flush=True)
        send(connection, TYPE_COMMAND, COMMAND_PING)
        pType, data = receive(connection)
        if(pType != TYPE_COMMAND or int.from_bytes(data, "big") != COMMAND_PONG): print("server did not pong ("+str(pType)+": "+str(data)+")")
        print("pong")

        while True:
            #check if there are more subtasks to be submitted
            if(nextSubtaskInput == None):
                nextSubtaskInput = next(inputData, None)
                if(nextSubtaskInput == None):
                    break
            
            #submit subtask
            if(nextSubtaskInput != None):
                print("submitting subtask...", end="", flush=True)
                send(connection, TYPE_COMMAND, COMMAND_SUBMITSUBTASK)
                pType, data = receive(connection)
                if(pType != TYPE_RESPONSE): print("server sent invalid response to submit subtask")
                response = int.from_bytes(data, "big")
                if(response == RESPONSE_OK):
                    send(connection, TYPE_DATA, nextSubtaskInput.encode())
                    pType, data = receive(connection)
                    if(pType != TYPE_DATA):
                        print("server did not send uuid")
                    else:
                        subtaskUUID = uuid.UUID(bytes=data)
                        pendingSubtasks[subtaskUUID] = nextSubtaskInput
                        nextSubtaskInput = None
                        print("submitted subtask "+str(subtaskUUID))
                elif(response == RESPONSE_NOTENOUGHSPACE):
                    print("queue full")
                    break
                else:
                    print("server sent unknown response to submit subtask")
            time.sleep(0.5)

        #check if any subtasks done
        while True:
            print("checking subtasks...", end="", flush=True)
            send(connection, TYPE_COMMAND, COMMAND_ISSUBTASKDONE)
            pType, data = receive(connection)
            if(pType != TYPE_RESPONSE): print("server sent invalid response to is subtask done")
            response = int.from_bytes(data, "big")
            if(response == RESPONSE_OK):
                pType, data = receive(connection)
                if(pType != TYPE_DATA):
                    print("server did not send uuid")
                else:
                    subtaskUUID = uuid.UUID(bytes=data)
                    pType, data = receive(connection)
                    if(pType != TYPE_DATA):
                        print("server did not send output")
                    else:
                        subtaskInput = pendingSubtasks.pop(subtaskUUID)
                        results[subtaskInput] = data.decode()
                        print("finished subtask "+str(subtaskUUID))
            elif(response == RESPONSE_NONEWRESULTS):
                print("no new results")
                break
            else:
                print("server sent unknown response to is subtask done")
            time.sleep(0.5)

        #check if done
        if(nextSubtaskInput == None and len(pendingSubtasks) == 0):
            send(connection, TYPE_COMMAND, COMMAND_EXIT)
            print("all subtasks finished")
            return results

        time.sleep(1)



# targetAddress = input("server ip address: ")
targetAddress = "192.168.50.221"
processor = "processor/process.py"
inputData = ["1\n2","a\na","q\nw","4\n4","5\n5","6\n6","7\n7","8\n8","9\n9","10\n2"]
# inputData = ["1\n2","a\na"]

outputData = runClient(targetAddress, processor, iter(inputData))
print(outputData)
f = open("clientOutput.txt", "w")
f.write(str(outputData))
f.close()
