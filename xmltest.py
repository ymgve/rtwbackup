

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
                
from xml.etree import ElementTree

f = JottaFolder(open("_foo.txt", "rb").read())
for fileobj in f.files:
    print fileobj.name

root = ElementTree.fromstring(open("_foo2.txt", "rb").read())

f = JottaFile(root)
print f.name
print f.currentRevision.number