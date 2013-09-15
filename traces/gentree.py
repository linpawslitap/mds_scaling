import random

def test():
  ar = random.SystemRandom(11)
  for i in range(0, 10000):
    print ar.expovariate(1/10.0)

def getNDir():
  global option
  return int(option['ndirgen'].expovariate(1 / option['meanndir']))

def getNFile():
  global option
  return int(option['nfilegen'].expovariate(1 / option['meannfile']))

def getFNLen():
  global option
  return int(option['nfngen'].gauss(option['meanfnlen'], option['stdfnlen']))

def getFilename():
  fnlen=getFNLen()
  alpha='abcdefghijklmnopqrstuvwxyz'
  fn=[]
  for i in range(0, fnlen):
    fn.append(alpha[random.randint(0,25)])
  return ''.join(fn)

def isRecursive(level):
  global option
  return option['nfngen'].random() < 1 / float(level + 1)


def gentree(cur, f, ndepth):
  global option, nentries
  if ndepth > option['maxdepth'] or nentries > option['maxentries']:
    return
  nfile = getNFile()
  for i in range(0, nfile):
    f.write(cur)
    f.write('/')
    f.write(getFilename())
    f.write(' f\n')
  ndir = getNDir()
  nentries += nfile + ndir
  for i in range(0, ndir):
    f.write(cur)
    f.write('/')
    dirname = getFilename()
    f.write(dirname)
    f.write(' d\n')
    if isRecursive(ndepth):
      gentree(cur+'/'+dirname, f, ndepth+1)

option = {
  'pdepth'   : float(1 / 10),
  'meanndir' : 30.0,
  'meannfile': 40.0,
  'meanfnlen': 13.0,
  'stdfnlen' : 5.0,
  'maxdepth' : 1000,
  'maxentries': 1000000,
  'ndirgen'  : random.SystemRandom(12),
  'nfilegen' : random.SystemRandom(123),
  'nfngen'   : random.SystemRandom(1234),
  'dgen'     : random.SystemRandom(12345)
}
nentries = 0
f = open("test.log", "w")
gentree("", f, 0)
f.close()
