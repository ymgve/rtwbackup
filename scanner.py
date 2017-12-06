import os, struct, zlib, hashlib, time, random, sys, logging, cStringIO

def walkerror(e):
    logger.exception("DIR ERROR %s" % str(e))

def randomfilename():
    return time.strftime("%Y-%m-%d_%H-%M-%S", time.gmtime(time.time()))

def write_io_to_file(sio, filename, mode):
    f = open(filename, mode)
    sio.seek(0)
    while True:
        block = sio.read(1024*1024)
        if len(block) == 0:
            break
            
        f.write(block)
        
    f.close()
        
def iter_meta_file(fullname):
    f = open(fullname, "rb")
    while True:
        fileinfo = f.read(32)
        if len(fileinfo) != 32:
            break
        
        filesize, timestamp, utfnamesize, numblocks, last_backup_timestamp = struct.unpack(">QQIIQ", fileinfo)
        utfname = f.read(utfnamesize)
        fullname = utfname.decode("utf8")
        
        blocks = [f.read(32) for i in xrange(numblocks)]
        
        yield fullname, filesize, timestamp, numblocks, last_backup_timestamp, blocks
        
    f.close()
    
logger = logging.getLogger("main")
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

fh = logging.FileHandler("backup.log")
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(formatter)

logger.addHandler(ch)
logger.addHandler(fh)

class BackupEngine(object):
    def __init__(self, backupdir):
        self.backupdir = unicode(backupdir)
        self.metadatadir = u"e:\\donotbackup"
        self.blocksize = 1024 * 1024
        self.blockfile_limit = 1024 * 1024 * 128
        self.knownblocks = {}
        self.blocks = 0
        self.dupeblocks = 0
        self.dupebytes = 0
        
        self.read_knownblocks()
        
        self.init_new_memory_files()
        
    def read_knownblocks(self):
        logger.debug("Starting reading known blocks indexes")
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
        logger.debug("Finished reading known blocks indexes")
                
    def init_new_memory_files(self):
        self.indexmem = cStringIO.StringIO()
        self.datmem = cStringIO.StringIO()
        self.metamem = cStringIO.StringIO()
        
        self.byteswritten = 0
        
    def flush_memory_files(self):
        logger.debug("flushing")
        if self.indexmem.tell() != 0:
            while True:
                prefix = "blocks_" + randomfilename() + ("_%08x" % random.randint(0, 0xffffffff))
                
                fullindex = os.path.join(self.metadatadir, prefix + ".index")
                fulldat = os.path.join(self.metadatadir, prefix + ".dat")
                if not os.path.isfile(fullindex):
                    break
                    
            write_io_to_file(self.indexmem, fullindex + ".temp", "wb")
            write_io_to_file(self.datmem, fulldat + ".temp", "wb")
            
            os.rename(fullindex + ".temp", fullindex)
            os.rename(fulldat + ".temp", fulldat)
            
        if self.metamem.tell() != 0:
            fullmeta = os.path.join(self.metadatadir, "backup.meta.temp")
            write_io_to_file(self.metamem, fullmeta, "ab")
    
        self.indexmem.close()
        self.datmem.close()
        self.metamem.close()
        logger.debug("flushing done")
            
    def do_backup(self):
        prevfullbackup = {}
        fullbackup = os.path.join(self.metadatadir, "backup.meta")
        if os.path.isfile(fullbackup):
            for fullname, filesize, timestamp, numblocks, last_backup_timestamp, blocks in iter_meta_file(fullbackup):
                prevfullbackup[fullname] = (filesize, timestamp)
                

        processedfiles = set()
        fullmeta = os.path.join(self.metadatadir, "backup.meta.temp")
        if os.path.isfile(fullmeta):
            for fullname, filesize, timestamp, numblocks, last_backup_timestamp, blocks in iter_meta_file(fullmeta):
                processedfiles.add(fullname)
                
        for dirpath, dirnames, filenames in os.walk(self.backupdir, onerror=walkerror):
            for filename in filenames:
                fullname = os.path.join(dirpath, filename)
                
                if fullname in processedfiles:
                    continue
                    
                try:
                    filesize = os.path.getsize(fullname)
                    timestamp = int(os.path.getmtime(fullname))
                except:
                    logger.exception("Failed to open file for backup %s" % repr(fullname))
                    continue
                    
                if fullname in prevfullbackup:
                    prevfilesize, prevtimestamp = prevfullbackup[fullname]
                    if prevfilesize == filesize and prevtimestamp == timestamp:
                        logger.debug("Skipping file because it hasn't changed since last full backup %s" % repr(fullname))
                        continue

                utfname = fullname.encode("utf8")
                
                current_time = int(time.time())
                
                hashes = self.backup_file(fullname)
                if hashes is not None:
                    self.metamem.write(struct.pack(">QQIIQ", filesize, timestamp, len(utfname), len(hashes), current_time))
                    self.metamem.write(utfname)
                    self.metamem.write("".join(hashes))
                    logger.info("done with file %s" % repr(fullname))
                    
        self.flush_memory_files()
            
        #os.rename(fullbackup, os.path.join(self.metadatadir, "backup_" + randomfilename() + ".meta"))
        
    def backup_file(self, fullname):
        hashes = []
        try:
            f = open(fullname, "rb")
        except:
            logger.exception("Failed to open file for backup %s" % repr(fullname))
            return None
            
        while True:
            try:
                block = f.read(self.blocksize)
            except:
                logger.exception("Failed to read from file %s" % repr(fullname))
                return None
            
            if len(block) == 0:
                break
                
            blockhash = hashlib.sha256(block).digest()
            hashes.append(blockhash)
            
            if blockhash not in self.knownblocks:
                zblock = zlib.compress(block, 1)
                self.datmem.write(zblock)
                # assume no one is crazy enough to use blocks of size 4GB+
                self.indexmem.write(blockhash + struct.pack(">II", len(block), len(zblock)))
                self.knownblocks[blockhash] = (None, len(block), len(zblock))
                self.byteswritten += len(zblock)
                if self.byteswritten >= self.blockfile_limit:
                    self.flush_memory_files()
                    self.init_new_memory_files()
            else:
                self.dupeblocks += 1
                self.dupebytes += len(block)
                
        try:
            f.close()
        except:
            logger.exception("??? Failed to close file %s" % repr(fullname))
        
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
            fileinfo = f.read(32)
            if len(fileinfo) != 32:
                break
            
            # TODO: Set time on files
            filesize, timestamp, utfnamesize, numblocks, last_backup_timestamp = struct.unpack(">QQIIQ", fileinfo)
            utfname = f.read(utfnamesize)
            fullname = utfname.decode("utf8")
            
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
                    of = open(targetpath, "r+b")
                    of.seek(targetoffset)
                    of.write(block)
                    of.close()
            


testdir = u"c:\\"
engine = BackupEngine(testdir)
engine.do_backup()
# engine.do_restore(u"e:\\ctf2", sys.argv[1])
