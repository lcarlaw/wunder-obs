import subprocess

def execute(arg):
    process = subprocess.Popen(arg, shell=True, stdout=subprocess.PIPE,
              stderr=subprocess.PIPE)
    process.communicate()
    return process

def rchop(s, suffix):
    if suffix and s.endswith(suffix):
        return s[:-len(suffix)]
    return s

#def du(path):
#    du = subprocess.check_output(['du','-sh', path]).split()[0].decode('utf-8')
#    return du
