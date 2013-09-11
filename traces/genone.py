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
  basedir = ""
  f.write("%d %d\n"%(nfiles, nfiles))
  for i in range(nfiles):
    f.write("%s f\n"%(basedir+"/"+get_pathname()))

gendirs(int(sys.argv[1]), sys.argv[2])
