import os, sys
import queue
import typing
import colorama
import threading
import socket
import time
import uuid
import datetime

#error logging
sys.stderr = open('error.log', 'w')

serverStartTime = time.time()



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
RESPONSE_SENDAUUID = 16  #algorithm uuid
RESPONSE_NOAUUID = 17

MAXSUBTASKS = 10  #max stored in server memory per client
SERVERFOLDER = "serverFiles"
VERBOSE = True



colorama.init()

clients : "list[socket.socket]" = []
nodes : "list[socket.socket]" = []
isServerShuttingDown = False

#dicts for clients (subtask UUID)
processingQueues : "dict[socket._RetAddress, queue.Queue[uuid.UUID]]" = dict()
resultQueues : "dict[socket._RetAddress, queue.Queue[uuid.UUID]]" = dict()
numTasksSubmitted : "dict[socket._RetAddress, int]" = dict()
numTasksDone : "dict[socket._RetAddress, int]" = dict()

#dicts for nodes
nodeHasTask : "dict[socket._RetAddress, bool]" = dict()
nodeSubTasks : "dict[socket._RetAddress, list[uuid.UUID]]" = dict()

#client UUID
addrToUUID : "dict[socket._RetAddress, uuid.UUID]" = dict()
UUIDToAddr : "dict[uuid.UUID, socket._RetAddress]" = dict()
UUIDToAUUID : "dict[uuid.UUID, uuid.UUID]" = dict()

#subtask UUID
UUIDToInOutData : "dict[uuid.UUID, typing.Tuple[bytes, bytes]]" = dict()

#code by fatal error in https://stackoverflow.com/a/28950776
def get_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.settimeout(0)
    try:
        # doesn't even have to be reachable
        s.connect(('10.255.255.255', 1))
        IP = s.getsockname()[0]
    except Exception:
        IP = '127.0.0.1'
    finally:
        s.close()
    return IP

serverIP = get_ip()

class SocketIsClosedException(Exception):
    pass  #dummy class for exiting when socket is closed
class GeneralSocketException(Exception):
    pass

def _receiveBytes(connection:socket.socket, numBytes):
    data = bytes()
    while len(data) < numBytes:
        newData = connection.recv(numBytes - len(data))
        if(len(newData) == 0):
            raise (SocketIsClosedException())
        data += newData
    return data

def receive(connection:socket.socket) -> typing.Tuple[int, bytes]:
    try:
        connectionAddr = connection.getpeername()
    except OSError:
        connectionAddr = "socket"
    try:
        lenghtAsBytes = _receiveBytes(connection, 4)
        length = int.from_bytes(lenghtAsBytes, "big")
        packetTypeAsBytes = _receiveBytes(connection, 4)
        packetType = int.from_bytes(packetTypeAsBytes, "big")
        data = _receiveBytes(connection, length)
        return (packetType, data)
    except SocketIsClosedException as e:
        addLineToDisplay(str(connectionAddr)+": socket closed")
        raise GeneralSocketException(e)
    except ConnectionResetError as e:
        addLineToDisplay(str(connectionAddr)+": connection reset")
        raise GeneralSocketException(e)
    except socket.timeout as e:
        addLineToDisplay(str(connectionAddr)+": socket timed out")
        raise GeneralSocketException(e)

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
        raise GeneralSocketException()

def startAccept(server:socket.socket):
    addLineToDisplay(str(server.getsockname())+": listening")
    while True:
        try:
            connection, _ = server.accept()
        except ConnectionAbortedError as e:
            if(isServerShuttingDown):
                return
            else:
                raise e
        addLineToDisplay(str(connection.getpeername())+": connected")
        connection.settimeout(MAXTIMEOUT)
        threading.Thread(None, handleNewConnection, None, [connection]).start()

def closeConnection(connection:socket.socket, message:str = None):
    try:
        if(message == None):
            addLineToDisplay(str(connection.getpeername())+": closing connection")
        else:
            addLineToDisplay(str(connection.getpeername())+": closing: "+str(message))
    except:
        if(message == None):
            addLineToDisplay("socket closed unexpectedly")
        else:
            addLineToDisplay("socket closed unexpectedly: "+str(message))
    connection.close()

def handleNewConnection(connection:socket.socket):
    connection.settimeout(MAXTIMEOUT)
    connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    connectionAddr = connection.getpeername()

    try:
        #wait for verfication bytes to confirm that it's not some random connection
        pType, data = receive(connection)
        assert pType == TYPE_HANDSHAKE and data == HANDSHAKEBYTES, "handshake failed"
        send(connection, TYPE_RESPONSE, RESPONSE_OK)
        #next packet indicates the type (node/client)
        pType, data = receive(connection)
        assert pType == TYPE_RESPONSE, "did not indicate connection type"
        response = int.from_bytes(data, "big")
        if(response == RESPONSE_CLIENT):
            addLineToDisplay(str(connectionAddr)+": registered as client")
            handleClient(connection)
        elif(response == RESPONSE_NODE):
            addLineToDisplay(str(connectionAddr)+": registered as node")
            handleNode(connection)
        else:
            raise AssertionError("not a node or a client")
        closeConnection(connection)
    except GeneralSocketException:
        closeConnection(connection, "socket error")
    except AssertionError as e:
        closeConnection(connection, e.args)
    addLineToDisplay(str(threading.current_thread().getName())+": thread ended")

clientThreadNameCounter = 0
def handleClient(connection:socket.socket):
    global clientThreadNameCounter

    connectionAddr = connection.getpeername()

    failed = False
    try:
        #load processor file
        pType, data = receive(connection)
        assert pType == TYPE_DATA, "didn't receive data (processor)"
        dataAsStr = data.decode()
        #generate and save into directory
        if(not os.path.isdir(SERVERFOLDER)):
            os.mkdir(SERVERFOLDER)
        clientFolder = os.path.join(SERVERFOLDER, str(connectionAddr))
        if(not os.path.isdir(clientFolder)):
            os.mkdir(clientFolder)
        clientUUID = uuid.uuid4()
        file = open(os.path.join(clientFolder, str(clientUUID)+".py"), "w")
        file.write(dataAsStr)
        file.close()
        addLineToDisplay(str(connectionAddr)+": received processor file")

        UUIDToAUUID[clientUUID] = None

        #check if more data to be sent
        while True:
            pType, data = receive(connection)
            assert pType == TYPE_RESPONSE, "didn't receive response"
            response = int.from_bytes(data, "big")
            if(response == RESPONSE_DONE):
                break
            elif(response == RESPONSE_SENDAUUID):
                pType, data = receive(connection)
                assert pType == TYPE_DATA, "didn't receive data (AUUID)"
                auuid = uuid.UUID(bytes=data)
                addLineToDisplay(str(connectionAddr)+": received AUUID")
                UUIDToAUUID[clientUUID] = auuid
            else:
                raise AssertionError("didn't receive RESPONSE_DONE")
    except GeneralSocketException:
        closeConnection(connection)
        return
    except AssertionError as e:
        closeConnection(connection, e.args)
        return

    addrToUUID[connectionAddr] = clientUUID
    UUIDToAddr[clientUUID] = connectionAddr
    
    clients.append(connection)
    threading.current_thread().setName("Client-"+str(clientThreadNameCounter)); clientThreadNameCounter += 1
    processingQueues[connectionAddr] = queue.Queue()
    resultQueues[connectionAddr] = queue.Queue()
    numTasksSubmitted[connectionAddr] = 0
    numTasksDone[connectionAddr] = 0
    try:
        while not isServerShuttingDown:
            pType, data = receive(connection)
            assert pType == TYPE_COMMAND, "didn't receive a command"
            command = int.from_bytes(data, "big")
            if(command == COMMAND_PING):
                send(connection, TYPE_COMMAND, COMMAND_PONG)
                continue
            elif(command == COMMAND_EXIT):
                addLineToDisplay(str(connectionAddr)+": received exit command")
                break
            elif(command == COMMAND_SUBMITSUBTASK):
                if(processingQueues[connectionAddr].qsize() > MAXSUBTASKS):
                    send(connection, TYPE_RESPONSE, RESPONSE_NOTENOUGHSPACE)
                else:
                    send(connection, TYPE_RESPONSE, RESPONSE_OK)
                    pType, data = receive(connection)
                    assert pType == TYPE_DATA, "didn't receive subtask data"
                    subtaskUUID = uuid.uuid4()
                    processingQueues[connectionAddr].put(subtaskUUID)
                    send(connection, TYPE_DATA, subtaskUUID.bytes)
                    numTasksSubmitted[connectionAddr] += 1
                    UUIDToInOutData[subtaskUUID] = (data, None)
                    if(VERBOSE):
                        addLineToDisplay(str(connectionAddr)+": submitted a subtask")
            elif(command == COMMAND_ISSUBTASKDONE):
                try:
                    subtaskUUID = resultQueues[connectionAddr].get(block=False)
                except queue.Empty:
                    send(connection, TYPE_RESPONSE, RESPONSE_NONEWRESULTS)
                    continue
                _, outputData = UUIDToInOutData.pop(subtaskUUID)
                send(connection, TYPE_RESPONSE, RESPONSE_OK)
                send(connection, TYPE_DATA, subtaskUUID.bytes)
                send(connection, TYPE_DATA, outputData)
            else:
                addLineToDisplay(str(connectionAddr)+": received unkown command ("+command+")")
    except GeneralSocketException:
        closeConnection(connection)
    except AssertionError as e:
        closeConnection(connection, e.args)
    clients.remove(connection)
    processingQueues.pop(connectionAddr)
    resultQueues.pop(connectionAddr)
    numTasksSubmitted.pop(connectionAddr)
    numTasksDone.pop(connectionAddr)
    k = addrToUUID.pop(connectionAddr)
    UUIDToAddr.pop(k)
    UUIDToAUUID.pop(clientUUID)

processingQueueThreads : "dict[socket._RetAddress, typing.List[threading.Thread]]" = dict()  #stores the threads that are processing each queue
#ensures that nodes are distributed evenly to tasks
#this is beneficial since it costs a lot of time to switch between tasks
taskDistributerMutex = threading.Lock()
def getTaskAddr():
    taskDistributerMutex.acquire()

    #ensure processingQueueThreads matches processingQueues
    for k in processingQueues.keys():
        if(k not in processingQueueThreads):
            processingQueueThreads[k] = []
    toRemove = []
    for k in processingQueueThreads.keys():
        if(k not in processingQueues):
            toRemove.append(k)
    for k in toRemove:
        processingQueueThreads.pop(k)

    #check to make sure thread info is correct
    curThread = threading.current_thread()
    for _, l in processingQueueThreads.items():
        for i in range(len(l)):
            if(not l[i].is_alive() or l[i] == curThread):
                l.remove(l[i])
                i -= 1
                
    #find queue with least threads handling it
    leastThreadsAddr = None
    leastThreadsNum = -1
    for addr in processingQueues.keys():
        if(processingQueues[addr].qsize() > 0 and (leastThreadsAddr == None or len(processingQueueThreads[addr]) < leastThreadsNum)):
            leastThreadsAddr = addr
            leastThreadsNum = len(processingQueueThreads[addr])
        
    taskDistributerMutex.release()
    return leastThreadsAddr  #will return None if there are no tasks to do

nodeThreadNameCounter = 0
def handleNode(connection:socket.socket):
    global nodeThreadNameCounter

    connectionAddr = connection.getpeername()

    nodes.append(connection)
    threading.current_thread().setName("Node-"+str(nodeThreadNameCounter)); nodeThreadNameCounter += 1
    nodeHasTask[connectionAddr] = False
    nodeSubTasks[connectionAddr] = []

    try:
        while not isServerShuttingDown:
            pType, data = receive(connection)
            assert pType == TYPE_COMMAND, "didn't receive a command"
            command = int.from_bytes(data, "big")
            if(command == COMMAND_PING):
                send(connection, TYPE_COMMAND, COMMAND_PONG)
                continue
            elif(command == COMMAND_EXIT):
                addLineToDisplay(str(connectionAddr)+": received exit command")
                break
            elif(command == COMMAND_GETTASK):
                addr = getTaskAddr()
                if(addr == None):
                    send(connection, TYPE_RESPONSE, RESPONSE_NONEWTASKS)
                else:
                    taskUUID = addrToUUID[addr]                    
                    algoUUID = UUIDToAUUID[taskUUID]

                    #send data to node
                    send(connection, TYPE_RESPONSE, RESPONSE_OK)
                    send(connection, TYPE_DATA, taskUUID.bytes)
                    if(algoUUID is not None):
                        send(connection, TYPE_RESPONSE, RESPONSE_SENDAUUID)
                        send(connection, TYPE_DATA, algoUUID.bytes)
                    else:
                        send(connection, TYPE_RESPONSE, RESPONSE_NOAUUID)

                    pType, data = receive(connection)
                    assert pType == TYPE_RESPONSE, "didn't receive response (has file)"
                    response = int.from_bytes(data, "big")
                    if(response == RESPONSE_OK):
                        if(VERBOSE):
                            addLineToDisplay(str(connectionAddr)+": is starting task "+str(taskUUID))
                        nodeHasTask[connectionAddr] = True
                    elif(response == RESPONSE_DOESNOTHAVEFILE):
                        #send file
                        processorFilePath = os.path.join(SERVERFOLDER, str(addr), str(taskUUID)+".py")
                        f = open(processorFilePath)
                        processorStr = f.read()
                        f.close()
                        send(connection, TYPE_DATA, processorStr.encode())
                        if(VERBOSE):
                            addLineToDisplay(str(connectionAddr)+": is starting task "+str(taskUUID)+" after receiving files")
                        nodeHasTask[connectionAddr] = True
                    else:
                        raise AssertionError("received unknown response ("+str(response)+")")
            elif(command == COMMAND_GETSUBTASK):
                pType, data = receive(connection)
                assert pType == TYPE_DATA, "didn't receive data (task uuid)"
                taskUUID = uuid.UUID(bytes=data)
                success = False
                try:
                    addr = UUIDToAddr[taskUUID]
                    q = processingQueues[addr]
                    subtaskUUID = q.get(block=False)
                    inputData, _ = UUIDToInOutData[subtaskUUID]
                    success = True
                except (KeyError, queue.Empty) as e:
                    send(connection, TYPE_RESPONSE, RESPONSE_NONEWSUBTASKS)
                    nodeHasTask[connectionAddr] = False
                if(success):
                    send(connection, TYPE_RESPONSE, RESPONSE_OK)
                    send(connection, TYPE_DATA, subtaskUUID.bytes)
                    send(connection, TYPE_DATA, inputData)
                    UUIDToAddr[subtaskUUID] = addr
                    if(VERBOSE):
                        addLineToDisplay(str(connectionAddr)+": is starting subtask "+str(subtaskUUID))
                    nodeSubTasks[connectionAddr].append(subtaskUUID)
            elif(command == COMMAND_SUBMITSUBTASKOUTPUT):
                pType, data = receive(connection)
                assert pType == TYPE_DATA, "didn't receive data (task uuid)"
                subtaskUUID = uuid.UUID(bytes=data)
                pType, data = receive(connection)
                assert pType == TYPE_DATA, "didn't receive data (output)"
                addr = UUIDToAddr.pop(subtaskUUID)
                resultQueues[addr].put(subtaskUUID)
                numTasksDone[addr] += 1
                UUIDToInOutData[subtaskUUID] = (None, data)
                if(VERBOSE):
                    addLineToDisplay(str(connectionAddr)+": finished subtask "+str(subtaskUUID))
                nodeSubTasks[connectionAddr].remove(subtaskUUID)
            else:
                raise AssertionError("received unknown command ("+command+")")
    except GeneralSocketException:
        closeConnection(connection)
    except AssertionError as e:
        closeConnection(connection, e.args)
    nodes.remove(connection)
    nodeHasTask.pop(connectionAddr)
    l = nodeSubTasks.pop(connectionAddr)
    #add them back to processing queue
    for subtaskUUID in l:
        processingQueues[UUIDToAddr[subtaskUUID]].put(subtaskUUID)

MAXMAXDISPLAYLINES = 10
maxDisplayLines = 10
displayLines = []

def addLineToDisplay(line):
    line = str(line)
    termSize = os.get_terminal_size()
    while(len(line) > termSize.columns):
        displayLines.append(line[0:termSize.columns])
        line = line[termSize.columns:]
    displayLines.append(line)
        
    while(len(displayLines) > maxDisplayLines):
        displayLines.pop(0)

def startDisplayLoop():
    print("\n"*os.get_terminal_size().lines)
    while not isServerShuttingDown:
        updateDisplay()
        time.sleep(0.5)

def updateDisplay():
    global maxDisplayLines

    termSize = os.get_terminal_size()

    print(colorama.Cursor.POS(0, 0)+" "*termSize.columns)
    for i in range(maxDisplayLines):
        if(i < len(displayLines)):
            print(displayLines[i].ljust(termSize.columns))
        else:
            print(" "*termSize.columns)
    lines : typing.List[str] = []
    lines.append("-"*termSize.columns)
    lines.append("server ip: "+str(serverIP))
    lines.append("active threads: {0:<5}    uptime: {1}".format(str(threading.active_count()), str(datetime.timedelta(seconds=time.time()-serverStartTime))))
    lines.append("")
    lines.append("nodes:")
    if(len(nodes) == 0):
        lines.append("  none")
    else:
        lines.append("  {0:<25}  {1:>10}  {2:>10}".format("address", "has task", "num STs"))
        for s in nodes:
            try:
                addr = s.getpeername()
                nht = nodeHasTask[addr]
                lnst = len(nodeSubTasks[addr])
            except OSError:
                addr = "error"
                nht = "..."
                lnst = "..."
            lines.append("  {0:<25}  {1:>10}  {2:>10}".format(str(addr), nht, lnst))
    lines.append("")
    lines.append("clients:")
    if(len(clients) == 0):
        lines.append("  none")
    else:
        lines.append("  {0:<25}  {1:>10}  {2:>10}  {3:>10}".format("address", "queue in", "queue out", "done"))
        for s in clients:
            try:
                addr = s.getpeername()
                pqs = processingQueues[addr].qsize() if addr in processingQueues else "..."  #processing queue size
                rqs = resultQueues[addr].qsize() if addr in resultQueues else "..."
                nts = numTasksSubmitted[addr]
                ntd = numTasksDone[addr]
            except OSError:
                addr = "error"
                pqs = "..."
                rqs = "..."
                nts = "..."
                ntd = "..."
            lines.append("  {0:<25}  {1:>10}  {2:>10}  {3:>10}".format(str(addr), pqs, rqs, str(ntd)+"/"+str(nts)))
    
    for l in lines:
        print(l.ljust(termSize.columns))

    #ensure new length of display doesn't exceed terminal size
    maxDisplayLines = min(termSize.lines - len(lines) - 2, MAXMAXDISPLAYLINES)

    #clear extra space under
    j = termSize.lines - len(lines) - 2 - maxDisplayLines
    while(j > 0):
        print(" "*termSize.columns)
        j -= 1



server = socket.socket()
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, True)
server.bind(("", PORT))
server.listen(100)
addLineToDisplay("server: setup done")

acceptThread = threading.Thread(None, startAccept, "Accept-Thread", [server])
uiThread = threading.Thread(None, startDisplayLoop, "UI-Thread")
acceptThread.start()
uiThread.start()
try:
    uiThread.join()
    print("ui thread exited")
    addLineToDisplay("ui thread exited")
    acceptThread.join()
except KeyboardInterrupt:
    print("keyboard interrupt: shutting down")
    addLineToDisplay("keyboard interrupt: shutting down")

#server is never meant to close, but in the case that the acceptThread ends for some reason, this is intended to close the connection
isServerShuttingDown = True
print("server: closing")
addLineToDisplay("server closing")
server.close()

updateDisplay()
