import sys
import random

def get_pathname():
  fnlen=20
  alpha='abcdefghijklmnopqrstuvwxyz' 
  fn=[]
  for i in range(0, fnlen):
    fn.append(alpha[random.randint(0,25)])
  return ''.join(fn)

def gendirs(nfiles, outfn):
  f = open(outfn, "w")
  basedir = "test"
  nfperdir = 128
  for i in range(nfiles / nfperdir):
    f.write("%s%d d\n"%(basedir, i))
  for i in range(nfiles / nfperdir):
    for j in range(nfperdir):
      f.write("%s%d/%s f\n"%(basedir, i, get_pathname()))
gendirs(int(sys.argv[1]), sys.argv[2])
