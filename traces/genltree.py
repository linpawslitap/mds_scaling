import random
import math

def test():
  ar = random.SystemRandom(11)
  for i in range(0, 10000):
    print ar.expovariate(1/10.0)

def getFilename():
    global option
    alpha='abcdefghijklmnopqrstuvwxyz'
    fn=[]
    for i in range(option['fnlen']):
        fn.append(alpha[random.randint(0,25)])
    return ''.join(fn)

def gentree(cur, ndepth, f1, f2):
    global option
    if ndepth > 1:
        f2.write(cur+'\n')
    if ndepth > option['maxdepth']:
        f1.write(cur+'\n');
        return
    if (ndepth == option['maxdepth']):
        width = option['lastwidth']
    else:
        width = option['width']
    for i in range(width):
        fn = getFilename();
        gentree(cur+'/'+fn, ndepth+1, f1, f2)

option = {
  'fnlen' : 12,
  'maxdepth' : 15,
  'width' : 2,
  'lastwidth': 2,
}

f1 = open("tree6.log", "w")
f2 = open("alltree6.log", "w")
f1.write("%d\n"%(math.pow(option['width'], option['maxdepth']-1)*option['lastwidth']))
f2.write("%d\n"%((math.pow(option['width'],option['maxdepth']+1)-option['width'])/(option['width']-1)))
gentree("", 1, f1, f2)
f1.close()
f2.close()
