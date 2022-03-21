import os
import sys
import time

inData = open("in.txt", "r")
sys.stdin = inData
outData = open("out.txt", "w")
sys.stdout = outData
sys.stderr = outData

x = input()
y = input()
time.sleep(5)
print(x + y)
