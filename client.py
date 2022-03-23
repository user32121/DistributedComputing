import os
import socket
import typing
import time
import uuid
import tqdm
import ast



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

CLIENTFOLDER = "clientFiles"



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
        tqdm.tqdm.write(str(connection.getpeername())+": socket closed")
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



def runClient(addr: str, processorFile: str, inputData: typing.Iterable[str], *, AUUID:uuid.UUID=None, checkpointFrequency=-1):
    inputData = iter(tqdm.tqdm(inputData, smoothing=0.1))

    if(not os.path.isdir(CLIENTFOLDER)):
        os.mkdir(CLIENTFOLDER)

    results : "dict[str, str]" = dict()

    #load checkpoint
    clientTempCheckpointPath = os.path.join(CLIENTFOLDER, "clientTempCheckpoint.txt")
    if(os.path.isfile(clientTempCheckpointPath)):
        tqdm.tqdm.write("loading results from clientTempCheckpoint.txt")
        clientTempCheckpoint = open(clientTempCheckpointPath, "r")
        prevCalculatedResults = clientTempCheckpoint.read()
        clientTempCheckpoint.close()
        results = ast.literal_eval(prevCalculatedResults)
    lastCheckpointAt = len(results)

    connection = socket.create_connection((addr, PORT))
    connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    tqdm.tqdm.write("connected to "+str(connection.getpeername())+" as "+str(connection.getsockname()))
    #handshake
    send(connection, TYPE_HANDSHAKE, HANDSHAKEBYTES)
    tqdm.tqdm.write("sent handshake")
    pType, data = receive(connection)
    assert pType == TYPE_RESPONSE, "server sent invalid response"
    assert int.from_bytes(data, "big") == RESPONSE_OK, "server did not send OK"
    tqdm.tqdm.write("server ok")
    #identify as client
    send(connection, TYPE_RESPONSE, RESPONSE_CLIENT)
    tqdm.tqdm.write("identified as client")

    #send preliminary data
    tqdm.tqdm.write("sending processor file")
    f = open(processorFile, "r")
    send(connection, TYPE_DATA, f.read().encode())
    f.close()
    tqdm.tqdm.write("file sent")
    if(AUUID is not None):
        tqdm.tqdm.write("sending AUUID "+str(AUUID))
        send(connection, TYPE_RESPONSE, RESPONSE_SENDAUUID)
        send(connection, TYPE_DATA, AUUID.bytes)
        tqdm.tqdm.write("sent AUUID")
    send(connection, TYPE_RESPONSE, RESPONSE_DONE)

    #send requests
    pendingSubtasks : "dict[uuid.UUID, str]" = dict()
    nextSubtaskInput : str = None
    tqdm.tqdm.write("starting processing")
    while True:
        #ping
        tqdm.tqdm.write("ping...", end="")
        # sys.stdout.flush()
        send(connection, TYPE_COMMAND, COMMAND_PING)
        pType, data = receive(connection)
        if(pType != TYPE_COMMAND or int.from_bytes(data, "big") != COMMAND_PONG): tqdm.tqdm.write("server did not pong ("+str(pType)+": "+str(data)+")")
        tqdm.tqdm.write("pong")

        while True:
            #check if there are more subtasks to be submitted
            if(nextSubtaskInput == None):
                nextSubtaskInput = next(inputData, None)
                if(nextSubtaskInput in results):
                    nextSubtaskInput = None
                    continue
                if(nextSubtaskInput == None):
                    break
            
            #submit subtask
            if(nextSubtaskInput != None):
                tqdm.tqdm.write("submitting subtask...", end="")
                # sys.stdout.flush()
                send(connection, TYPE_COMMAND, COMMAND_SUBMITSUBTASK)
                pType, data = receive(connection)
                if(pType != TYPE_RESPONSE): tqdm.tqdm.write("server sent invalid response to submit subtask")
                response = int.from_bytes(data, "big")
                if(response == RESPONSE_OK):
                    send(connection, TYPE_DATA, nextSubtaskInput.encode())
                    pType, data = receive(connection)
                    if(pType != TYPE_DATA):
                        tqdm.tqdm.write("server did not send uuid")
                    else:
                        subtaskUUID = uuid.UUID(bytes=data)
                        pendingSubtasks[subtaskUUID] = nextSubtaskInput
                        nextSubtaskInput = None
                        tqdm.tqdm.write("submitted subtask "+str(subtaskUUID))
                elif(response == RESPONSE_NOTENOUGHSPACE):
                    tqdm.tqdm.write("queue full")
                    time.sleep(MAXTIMEOUT / 2)
                    break
                else:
                    tqdm.tqdm.write("server sent unknown response to submit subtask")
            time.sleep(0.5)

        #check if any subtasks done
        while True:
            tqdm.tqdm.write("checking subtasks...", end="")
            # sys.stdout.flush()
            send(connection, TYPE_COMMAND, COMMAND_ISSUBTASKDONE)
            pType, data = receive(connection)
            if(pType != TYPE_RESPONSE): tqdm.tqdm.write("server sent invalid response to is subtask done")
            response = int.from_bytes(data, "big")
            if(response == RESPONSE_OK):
                pType, data = receive(connection)
                if(pType != TYPE_DATA):
                    tqdm.tqdm.write("server did not send uuid")
                else:
                    subtaskUUID = uuid.UUID(bytes=data)
                    pType, data = receive(connection)
                    if(pType != TYPE_DATA):
                        tqdm.tqdm.write("server did not send output")
                    else:
                        subtaskInput = pendingSubtasks.pop(subtaskUUID)
                        subtaskOutput = data.decode()
                        results[subtaskInput] = subtaskOutput
                        tqdm.tqdm.write("finished subtask "+str(subtaskUUID)+": "+subtaskInput+" -> "+results[subtaskInput])
            elif(response == RESPONSE_NONEWRESULTS):
                tqdm.tqdm.write("no new results")
                time.sleep(MAXTIMEOUT / 2)
                break
            else:
                tqdm.tqdm.write("server sent unknown response to is subtask done")
            time.sleep(0.5)

        #checkpoints
        if(len(results) > lastCheckpointAt + checkpointFrequency):
            tqdm.tqdm.write("writing checkpoint at "+str(len(results)))
            clientTempCheckpoint = open(clientTempCheckpointPath, "w")
            clientTempCheckpoint.write(str(results))
            clientTempCheckpoint.close()
            while(lastCheckpointAt <= len(results)):
                lastCheckpointAt += checkpointFrequency
            

        #check if done
        if(nextSubtaskInput == None and len(pendingSubtasks) == 0):
            send(connection, TYPE_COMMAND, COMMAND_EXIT)
            tqdm.tqdm.write("all subtasks finished")
            return results

        time.sleep(1)



targetAddress = input("server ip address: ")

if(False):
    #temp processor
    processor = "processor/process.py"
    inputData = ["1\n2","a\na","q\nw","4\n4","5\n5","6\n6","7\n7","8\n8","9\n9","10\n2"]
    # inputData = ["1\n2","a\na"]
    AUUID = None
else:
    #nerdle
    processor = "processor/nerdleSolver1DC.py"
    f = open("processor/equations3.txt")
    inputData = f.read()
    f.close()
    inputData = inputData.strip().split("\n")
    AUUID = uuid.UUID('aa9df30a-eb04-42eb-9c2c-8059edcaa7ea')

outputData = runClient(targetAddress, processor, inputData, AUUID=AUUID, checkpointFrequency=10)
print(outputData)
f = open(os.path.join(CLIENTFOLDER, "clientOutput.txt"), "w")
f.write(str(outputData))
f.close()
