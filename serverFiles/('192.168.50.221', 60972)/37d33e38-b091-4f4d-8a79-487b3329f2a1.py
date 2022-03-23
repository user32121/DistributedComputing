#this version generates all possible one-sided equations according to the nerdle rules
#this has as few equations as possible to reduce computation time

import tqdm
import os, re

EQUATIONLENGTH = 8
NUMCHARS = 15
NUMNUMBERS = 10
NUMOPERATORS = 5
NUMBERS = ["0","1","2","3","4","5","6","7","8","9"]
OPERATORS = ["+","-","*","/","="]

CHARTONUM = {
    "0": 0,
    "1": 1,
    "2": 2,
    "3": 3,
    "4": 4,
    "5": 5,
    "6": 6,
    "7": 7,
    "8": 8,
    "9": 9,
    "+": 10,
    "-": 11,
    "*": 12,
    "/": 13,
    "=": 14,
}
NUMTOCHAR = {
    0: "0",
    1: "1",
    2: "2",
    3: "3",
    4: "4",
    5: "5",
    6: "6",
    7: "7",
    8: "8",
    9: "9",
    10: "+",
    11: "-",
    12: "*",
    13: "/",
    14: "=",
}
SYMBOLTOPRECEDENCE = {
    "+": 1,
    "-": 1,
    "*": 2,
    "/": 2
}



class DummyIterator():
    def __init__(self) -> None:
        pass
    def __iter__(self):
        return self
    def __next__(self) -> None:
        pass

def validateEquation(equation):
    equationStr = "".join(equation)
    equationStr = equationStr.split("=")[0]
    if(re.search("(?<![0-9])0+([1-9])", equationStr) != None):  #no leading 0s
        return False
    try:
        value = eval(equationStr)
    except ZeroDivisionError:  #cannot divide by 0
        return False
    if(int(value) != value):  #cannot be a float
        return False
    s = str(int(value))
    if(len(s)+len(equationStr)+1 != EQUATIONLENGTH):
        return False
    for i in range(len(s)):
        equation[-1-i] = s[-1-i]
    return True

generateEquationsIterator = None
def generateAllEquations():
    global generateEquationsIterator
    generateEquationsIterator = tqdm.tqdm(DummyIterator())
    generateEquationsIterator.desc = "total equations"
    generateEquationsIterator.display()
    generateEquationsIterator = iter(generateEquationsIterator)
    workspace = [""]*EQUATIONLENGTH
    totalCollection = []
    selectNum(0, workspace, totalCollection, 2)
    generateEquationsIterator.close()
    return totalCollection

def selectNum(curIndex, workspace, totalCollection, progressBarDepth=0):
    if(curIndex >= EQUATIONLENGTH-1):
        return
    if(progressBarDepth):
        toIterate = tqdm.tqdm(NUMBERS)
        toIterate.desc = str(curIndex)+": num"
        toIterate.display()
        progressBarDepth -= 1
    else:
        toIterate = NUMBERS
    for x in toIterate:
        workspace[curIndex] = x
        selectNum(curIndex+1, workspace, totalCollection, progressBarDepth)
        selectOperator(curIndex+1, workspace, totalCollection, progressBarDepth)

def selectOperator(curIndex, workspace, totalCollection, progressBarDepth=0):
    global generateEquationsIterator
    if(curIndex >= EQUATIONLENGTH-1):
        return
    if(progressBarDepth):
        toIterate = tqdm.tqdm(OPERATORS)
        toIterate.desc = str(curIndex)+": opr"
        toIterate.display()
        progressBarDepth -= 1
    else:
        toIterate = OPERATORS
    for x in toIterate:
        workspace[curIndex] = x
        if(x == "="):
            if(validateEquation(workspace)):
                totalCollection.append(wordToNums(workspace))
                next(generateEquationsIterator)
        else:
            selectNum(curIndex+1, workspace, totalCollection, progressBarDepth)




RESULT_INVALID = -1
RESULT_WRONG = 1          #letter not in word
RESULT_WRONGPLACE = 2     #letter exists but in incorrect place
RESULT_CORRECT = 3        #letter in correct place

def create2DArray():
    l = [None]*EQUATIONLENGTH
    for i in range(len(l)):
        l[i] = [None]*NUMCHARS
    return l

curKnownLetters = create2DArray()  #[position][letter]
curKnownNumLetters = [None]*NUMCHARS
curKnownIsExactNum = [False]*NUMCHARS

def copy2DArray(template):
    l = [None]*len(template)
    for i in range(len(l)):
        l[i] = template[i].copy()
    return l

def generatePossibleRes():
    l = []
    generatePossibleResHelper([None]*EQUATIONLENGTH, 0, [RESULT_WRONG,RESULT_WRONGPLACE,RESULT_CORRECT], l)
    return l

def generatePossibleResHelper(curState, curIndex, itemsToChooseFrom, totalCollection):
    for x in itemsToChooseFrom:
        curState[curIndex] = x
        if(curIndex+1 == len(curState)):
            totalCollection.append(curState.copy())
        else:
            generatePossibleResHelper(curState, curIndex+1, itemsToChooseFrom, totalCollection)

def resToIndex(res):
    return res[4]-1 + 3*(res[3]-1 + 3*(res[2]-1 + 3*(res[1]-1 + 3*(res[0]-1))))

def wordToNums(word):
    l = []
    for c in word:
        if(type(c) == int):
            l.append(c)
        else:
            l.append(CHARTONUM[c])
    return l

def numsToWord(word):
    s = ""
    for x in word:
        s += NUMTOCHAR[x]
    return s

def isValidWord(knownLetters, knownNumLetters, knownIsExactNum, word):
    numLetters = [0]*NUMCHARS
    for i in range(len(word)):
        if(i > 0 and word[i-1] == 0 and word[i] >= 0 and word[i] <= 9):  #leading zero
            return False
        numLetters[word[i]] += 1
        if(knownLetters[i][word[i]] == False):  #letter known to not be here
            return False
    for i in range(NUMCHARS):  #incorrect number of letters
        if(knownNumLetters[i] != None and numLetters[i] < knownNumLetters[i] or (numLetters[i] != knownNumLetters[i] and knownIsExactNum[i])):
           return False 
    return True

#returns True if succeeds, False if there was a warning
def UpdateBasedOnRes(knownLetters, knownNumLetters, knownIsExactNum, guess, res, printWarnings=True):
    resultNumLetters = [0]*NUMCHARS
    for i in range(EQUATIONLENGTH):
        if(res[i] == RESULT_CORRECT):
            resultNumLetters[guess[i]] += 1
        elif(res[i] == RESULT_WRONGPLACE):
            resultNumLetters[guess[i]] += 1

    hasNoWarning = True
    for i in range(len(res)):
        if(res[i] == RESULT_CORRECT):
            if (knownLetters[i][guess[i]] == False):
                if(printWarnings):
                    print("WARNING(1): conflicting results for "+NUMTOCHAR[guess[i]]+" at "+str(i)+" ("+numsToWord(guess)+")")
                hasNoWarning = False
            knownLetters[i][guess[i]] = True
            knownIsExactNum[guess[i]] = False
            #remove other possible letters in this position
            for j in range(NUMCHARS):
                if(j != guess[i]):
                    knownLetters[i][j] = False
        elif(res[i] == RESULT_WRONGPLACE):
            if (knownLetters[i][guess[i]] == True):
                if(printWarnings):
                    print("WARNING(2): conflicting results for "+NUMTOCHAR[guess[i]]+" at "+str(i)+" ("+numsToWord(guess)+")")
                hasNoWarning = False
            knownLetters[i][guess[i]] = False
            knownIsExactNum[guess[i]] = False
        elif(res[i] == RESULT_WRONG):
            if (knownLetters[i][guess[i]] == True):
                if(printWarnings):
                    print("WARNING(3): conflicting results for "+NUMTOCHAR[guess[i]]+" at "+str(i)+" ("+numsToWord(guess)+")")
                hasNoWarning = False
            knownLetters[i][guess[i]] = False
            knownNumLetters[guess[i]] = resultNumLetters[guess[i]]
            knownIsExactNum[guess[i]] = True
            #count to see if number of found letters matches numLetters
            num = 0
            for j in range(EQUATIONLENGTH):
                if(knownLetters[j][guess[i]] == True):
                    num += 1
            #remove other possible arrangements if all current ones are known
            if(num == knownNumLetters[guess[i]]):
                for j in range(EQUATIONLENGTH):
                    if(knownLetters[j][guess[i]] == None):
                        knownLetters[j][guess[i]] = False
        else:
            raise Exception(res[i]+" result type is not implemented")
        
    #set the minimum number of letters found if not exact number found
    for i in range(NUMCHARS):
        if(resultNumLetters[i] != 0 and knownIsExactNum[i] == False):
            knownNumLetters[i] = resultNumLetters[i]
    return hasNoWarning

def TryWord(guess, actualWord):
    assert(len(actualWord) == len(guess))
    
    res = [None]*len(actualWord)
    actualNumLetters = [0]*NUMCHARS
    #count to check if correct letters but wrong position
    for i in range(len(actualWord)):
        actualNumLetters[actualWord[i]] += 1
    for i in range(len(actualWord)):
        if(actualWord[i] == guess[i]):
            res[i] = RESULT_CORRECT
            actualNumLetters[actualWord[i]] -= 1
        elif(actualNumLetters[actualWord[i]] > 0):
            res[i] = RESULT_WRONGPLACE
            actualNumLetters[actualWord[i]] -= 1
        else:
            res[i] = RESULT_WRONG

    return res

def getGuessAndRes():
    print("guess:")
    while(True):
        guess = wordToNums(input())
        if(guess in words):
            break
        else:
            print("invalid")
            if(input("are you sure you want to use this? (y/n)") == "y"):
                break
    
    print("result:")
    while(True):
        resNum = input()
        if (len(resNum) != len(guess)):
            print("invalid")
            continue

        res = [None]*EQUATIONLENGTH
        failed = False
        for i in range(len(guess)):
            num = ord(resNum[i]) - ord('0')
            if (num >= 1 and num <= 3):
                res[i] = num
            else:
                print("invalid")
                failed = True
                break
        if(failed):
            continue
        
        return (guess, res)

#header
print("1 - wrong")
print("2 - wrong place")
print("3 - correct")
print("----------")

#preprocess
if(os.path.isfile("equations3.txt")):
    print("using equations3.txt...")
    f = open("equations3.txt", "r")
    words = f.readlines()
    f.close()
else:
    print("generating equations...")
    words = generateAllEquations()
    words = [numsToWord(w)+"\n" for w in words]
    print("saving to equations3.txt")
    f = open("equations3.txt", "w")
    f.writelines(words)
    f.close()
# print(words)
words = [wordToNums(w.strip()) for w in words]
print(str(len(words))+" words")
print("done preprocessing")

numTries = 0

#start
while True:
    #find all valid words
    validWords = []
    for w in words:
        if(isValidWord(curKnownLetters, curKnownNumLetters, curKnownIsExactNum, w)):
            validWords.append(w)
    wordsAsStr = []
    for w in validWords:
        wordsAsStr.append(numsToWord(w))
    print(wordsAsStr)
    print("valid words: "+str(len(validWords)))
    if(len(validWords) == 0):
        print("unknown word")
        break
    elif(len(validWords) == 1):
        print("found word after "+str(numTries)+" tries")
        break

    #get user input
    (guess, res) = getGuessAndRes()

    #update known info
    UpdateBasedOnRes(curKnownLetters, curKnownNumLetters, curKnownIsExactNum, guess, res)
    numTries += 1
