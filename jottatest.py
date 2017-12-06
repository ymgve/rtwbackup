import requests, time, os, hashlib, logging

from requests_toolbelt.multipart import encoder

from xml.etree import ElementTree

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)


class JottaFileRevision(object):
    def __init__(self, root):
        for node in root:
            setattr(self, node.tag, node.text)

class JottaFile(object):
    def __init__(self, root):
        if type(root) is str:
            root = ElementTree.fromstring(root)
            
        self.currentRevision = None
        self.latestRevision = None
        self.revisions = []
        
        assert root.tag == "file"
        for key in root.attrib:
            setattr(self, key, root.attrib[key])
            
        for node in root:
            if node.tag == "currentRevision":
                self.currentRevision = JottaFileRevision(node)
                
            elif node.tag == "latestRevision":
                self.latestRevision = JottaFileRevision(node)
            
            elif node.tag == "revisions":
                self.revisions = [JottaFileRevision(node2) for node2 in node]
        
class JottaFolder(object):
    def __init__(self, root):
        if type(root) is str:
            root = ElementTree.fromstring(root)
            
        for key in root.attrib:
            setattr(self, key, root.attrib[key])
            
        self.files = []
        for node in root:
            if node.tag == "files":
                self.files = [JottaFile(node2) for node2 in node]

class Jotta(object):
    def __init__(self, username, password, path):
        self.username = username
        self.password = password
        self.path = path
        self.baseurl = "https://www.jottacloud.com/jfs/%s/Jotta/%s" % (self.username, self.path)
        
    def list_files(self):
        res = requests.get(self.baseurl, auth=(self.username, self.password))
        print res.content
        
    def verify_upload_response(self, jf, remotename, md5, filesize):
        if jf.currentRevision is not None:
            if jf.currentRevision.state == "COMPLETED":
                if jf.name != remotename:
                    raise Exception("File mismatch in name local %s remote %s" % (remotename, jf.name))
                    
                if jf.currentRevision.md5 != md5:
                    raise Exception("MD5 mismatch in name local %s remote %s" % (md5, jf.currentRevision.md5))
                    
                if int(jf.currentRevision.size) != filesize:
                    raise Exception("Size mismatch in name local %s remote %s" % (size, jf.currentRevision.size))
                    
                return True
                
        return False
    
    def upload(self, fullname, remotename=None, md5=None):
        logger.info("Starting upload of file %s" % fullname)
        if md5 is None:
            f = open(fullname, "rb")
            h = hashlib.md5()
            while True:
                block = f.read(1024*1024)
                if len(block) == 0:
                    break
                h.update(block)
            md5 = h.hexdigest()

        logger.debug("MD5 is %s" % md5)
        
        if remotename is None:
            remotename = os.path.basename(fullname)
            
        filesize = os.path.getsize(fullname)
        
        upload_url = self.baseurl + "/" + remotename
        
        res = requests.get(upload_url, auth=(self.username, self.password))
        logger.debug("response %s %s" % (res, res.content))
        if res.status_code == 200:
            jf = JottaFile(res.content)
            if self.verify_upload_response(jf, remotename, md5, filesize):
                return True
            elif jf.latestRevision is not None:
                if jf.latestRevision.state == "INCOMPLETE":
                    resume_offset = int(jf.latestRevision.size)
                    localfile = open(fullname, "rb")
                    localfile.seek(resume_offset)
                else:
                    raise Exception("Unknown file state %s" % jf.latestRevision.state)
            else:
                raise Exception("Weird XML stuff!")
                    
        elif res.status_code == 404:
            localfile = open(fullname, "rb")
            resume_offset = 0
        else:
            raise Exception("Unknown HTTP status code before upload %d" % res.status_code)
        
        e = encoder.MultipartEncoder({"file": (remotename, localfile, "application/octet-stream")})

        headers = {
                "JSize": str(filesize),
                "JMd5": md5,
                "Content-Type": e.content_type
            }

        if resume_offset > 0:
            headers["Content-Range"] = "bytes %d-%d/%d" % (resume_offset, filesize - 1, filesize)
            
        res = requests.post(upload_url, data=e, headers=headers, auth=(self.username, self.password))
        logger.debug("response %s %s" % (res, res.content))
        if res.status_code != 201:
            raise Exception("bad status code %d" % res.status_code)
            
        jf = JottaFile(res.content)
        if self.verify_upload_response(jf, remotename, md5, filesize):
            return True
            
        raise Exception("File upload went wrong!")

# start = time.time()
#res = requests.get("https://www.jottacloud.com/jfs/ymgve/Jotta/Archive/test/test3.data?mode=bin", auth=("", ""))

# res = requests.post("https://www.jottacloud.com/jfs/ymgve/Jotta/Archive/test/test5.data", auth=(), data=m, headers=headers)

# res = requests.get("https://www.jottacloud.com/jfs/ymgve/Jotta/Archive/test/test5.data", auth=("", ""))

# print res.content
# print time.time() - start
#root = ET.fromstring(res.content)
# print root.tag
#res = requests.put("https://www.jottacloud.com/jfs/ymgve/Jotta/Archive/test", auth=("", ""))

jotta = Jotta("", "", "Archive/test")
#jotta.list_files()
jotta.upload("E:\\dump2016\\AM2R_10.zip", "aaaxz")