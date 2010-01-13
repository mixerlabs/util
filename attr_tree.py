"""
A simple tree structure where attributes can be stored along the nodes. 
Each node is of the form [<name>, <attributes>, <children>] -> ["", {}, []]
"""
import simplejson

class AttrTree(object):
    def __init__(self, serialized=None):
        """
        Initialize a tree object. if serialized is set, the tree is initialized
        based on a tree object it expects in JSON format.
        """
        if serialized != None:
            self.root = simplejson.loads(serialized)
        else:
            self.root = {}
        self._cache = {}
        
    
    def serialize(self, node=None):
        """
        Returns a JSON-dump-formatted representation of the tree.
        """
        if node is None:
            node = self.root
        return simplejson.dumps(node, ensure_ascii=False)

    def get(self, path):
        """
        Returns the node identified by the path param.
        """
        if type(path) != tuple:
            raise AttributeError("Path of type tuple required (path was %s)" % (path, ))
        if path in self._cache:
            return self._cache[path]
        node = [self.root, {}]
        last = len(path) - 1 
        index = 0
        while index < len(path):
            if index == last and path[index] in node[1]:
                return node[1][path[index]]
            if path[index] not in node[0]:
                return None
            node = node[0][path[index]]
            index += 1
        self._cache[path] = node
        return node
    
    def set(self, path, attributes):
        """
        Inserts nodes if needed to construct the specified path and 
        sets the attributes at the end of the path.
        """
        if type(path) != tuple or len(path) == 0:
            raise AttributeError("Path of type tuple with length > 0 required (path was %s)" % (path, ))
        nodes = self.root
        index = 0
        while index < len(path):
            child = nodes.setdefault(path[index], [{}, {}])
            nodes = child[0]
            index += 1
        child[1].update(attributes)
        


