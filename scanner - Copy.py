import os, struct, zlib, hashlib, time, random, sys, logging, cStringIO

def walkerror(e):
    if type(e) is WindowsError:
        print "DIR ERROR", str(e)

def write_with_size(f, s):
    f.write(struct.pack(">I", len(s)))
    f.write(s)
    
def randomfilename():
    return time.strftime("%Y-%m-%d_%H-%M-%S", time.gmtime(time.time()))
    
logger = logging.getLogger('main')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

class BackupEngine(object):
    def __init__(self, backupdir):
        self.backupdir = unicode(backupdir)
        self.metadatadir = u"c:\\temp"
        self.tempdir = u"c:\\temp\\temp"
        self.blocksize = 1024 * 1024
        self.blockfile_limit = 1024 * 1024 * 128
        self.knownblocks = {}
        self.blocks = 0
        self.dupeblocks = 0
        self.dupebytes = 0
        
        self.read_knownblocks()
        
        self.init_new_block_file()
        
    def read_knownblocks(self):
        for filename in os.listdir(self.metadatadir):
            if filename.endswith(".index"):
                offset = 0
                fullname = os.path.join(self.metadatadir, filename)
                f = open(fullname, "rb")
                while True:
                    hash = f.read(32)
                    if len(hash) != 32:
                        break
                        
                    usize, csize = struct.unpack(">II", f.read(8))
                    self.knownblocks[hash] = (filename, offset, usize, csize)
                    offset += csize
                
    def init_new_block_file(self):
        while True:
            prefix = "blocks_" + randomfilename() + ("_%08x" % random.randint(0, 0xffffffff))
            self.indexname = prefix + ".index"
            self.datname = prefix + ".dat"
            self.fullindex = os.path.join(self.tempdir, self.indexname + ".temp")
            self.fulldat = os.path.join(self.tempdir, self.datname + ".temp")
            if not os.path.isfile(self.fullindex):
                break
        
        self.byteswritten = 0
        
    def move_block_file(self):
        if os.path.isfile(self.fullindex) and os.path.isfile(self.fulldat):
            os.rename(self.fullindex, os.path.join(self.metadatadir, self.indexname))
            os.rename(self.fulldat, os.path.join(self.metadatadir, self.datname))
        
    def do_backup(self):
        backupname = "backup.meta.temp"
        fullbackup = os.path.join(self.tempdir, backupname + ".temp")
        
        # processedfiles = {}
        # if os.path.isfile(fullbackup):
            # f = open(fullbackup, "rb")
            # while True:
                # fileinfo = f.read(24)
                # if len(fileinfo) != 24:
                    # break
                
            # filesize, timestamp, utfnamesize, numblocks, last_backup_timestamp = struct.unpack(">QQIIQ", fileinfo)
            # utfname = f.read(utfnamesize)
            # fullname = utfname.decode("utf8")
            
            # blocks = []
            # for i in xrange(numblocks):
                # hash = f.read(32)
                # blocks.append(hash)
            
            # knownfiles[fullname] = (filesize, timestamp, utfnamesize, numblocks, last_backup_timestamp, blocks)
            
        # f.close()
        
        f = open(fullbackup, "wb")
        
        for dirpath, dirnames, filenames in os.walk(self.backupdir, onerror=walkerror):
            for filename in filenames:
                fullname = os.path.join(dirpath, filename)
                
                try:
                    filesize = os.path.getsize(fullname)
                    timestamp = int(os.path.getmtime(fullname))
                except:
                    logger.exception("Failed to open file for backup %s" % repr(fullname))
                    continue
                    
                utfname = fullname.encode("utf8")
                
                current_time = int(time.time())
                
                hashes = self.backup_file(fullname)
                if hashes is not None:
                    f.write(struct.pack(">QQIIQ", filesize, timestamp, len(utfname), len(hashes), current_time))
                    f.write(utfname)
                    f.write("".join(hashes))
                    print "done", repr(fullname)
                    
        self.move_block_file()
            
        f.close()
        os.rename(fullbackup, os.path.join(self.metadatadir, "backup_" + randomfilename() + ".meta"))
        
    def backup_file(self, fullname):
        hashes = []
        try:
            f = open(fullname, "rb")
        except:
            logger.exception("Failed to open file for backup %s" % repr(fullname))
            return None
            
        while True:
            block = f.read(self.blocksize)
            if len(block) == 0:
                break
                
            blockhash = hashlib.sha256(block).digest()
            hashes.append(blockhash)
            
            if blockhash not in self.knownblocks:
                zblock = zlib.compress(block, 1)
                open(self.fulldat, "ab").write(zblock)
                # assume no one is crazy enough to use blocks of size 4GB+
                open(self.fullindex, "ab").write(blockhash + struct.pack(">II", len(block), len(zblock)))
                self.knownblocks[blockhash] = (self.indexname, len(block), len(zblock))
                self.byteswritten += len(zblock)
                if self.byteswritten >= self.blockfile_limit:
                    self.move_block_file()
                    self.init_new_block_file()
            else:
                self.dupeblocks += 1
                self.dupebytes += len(block)
                
        f.close()
        
        return hashes
                        
    def do_restore(self, targetpath, metafile):
        targetpath = unicode(targetpath)
        metafile = unicode(metafile)
        
        sortedblocks = []
        for hash in self.knownblocks:
            blockfilename, offset, usize, csize = self.knownblocks[hash]
            sortedblocks.append((blockfilename, offset, usize, csize, hash))
            
        sortedblocks.sort()

        files_with_block = {}
        
        f = open(metafile, "rb")
        while True:
            fileinfo = f.read(24)
            if len(fileinfo) == 0:
                break
            
            # TODO: Set time on files
            filesize, timestamp, utfnamesize, numblocks, current_time = struct.unpack(">QQIIQ", fileinfo)
            utfname = f.read(utfnamesize)
            fullname = utfname.decode("utf8")
            
            #print filesize, repr(fullname)
            targetname = u"\\\\?\\" + os.path.join(targetpath, fullname.replace(":", ""))
            
            offset = 0
            blocks = []
            for i in xrange(numblocks):
                hash = f.read(32)
                if hash not in files_with_block:
                    files_with_block[hash] = []
                files_with_block[hash].append((targetname, offset))
                offset += self.knownblocks[hash][2]

            try:
                os.makedirs(os.path.dirname(targetname))
            except:
                pass
                
            of = open(targetname, "wb")
            if filesize > 0:
                of.seek(filesize - 1)
                of.write("\x00")
            of.close()
            
        f.close()
        
        currblock = None
        for blockfilename, offset, usize, csize, hash in sortedblocks:
            print blockfilename, offset, usize, csize, hash.encode("hex")
            if currblock != blockfilename:
                f = open(os.path.join(self.metadatadir, blockfilename.replace(".index", ".dat")), "rb")
                currblock = blockfilename
                
            if hash in files_with_block:
                f.seek(offset)
                zblock = f.read(csize)
                block = zlib.decompress(zblock)
                assert hashlib.sha256(block).digest() == hash
                for targetpath, targetoffset in files_with_block[hash]:
                    print targetpath, targetoffset
                    of = open(targetpath, "r+b")
                    of.seek(targetoffset)
                    of.write(block)
                    of.close()
            


testdir = u"e:\\ctf"
engine = BackupEngine(testdir)
engine.do_backup()
# engine.do_restore(u"e:\\ctf2", sys.argv[1])
