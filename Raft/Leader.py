from Role import Role

class Leader:
    def __init__(self, nodeId, nodes):
        # The first four values need to stored on disk (stable storage)
        self.currentTerm = 0
        self.votedFor = None
        self.log = []
        self.commitLength = 0
        self.currentRole = Role.Follower
        self.currentLeader = None
        self.votesReceived = set()
        self.sentLength = []
        self.ackedLength = []
        self.nodeId = nodeId
        self.nodes = nodes

    def broadcastMessage(self, msg):
        if self.currentRole == Role.Leader:
            self.log({"msg": msg, "term": self.currentTerm})
            self.ackedLength[self.nodeId] = len(self.log)
            for follower in self.nodes:
                self.relicateLog(self.nodeId, follower)

        else:
            # forward the request to currentLeader via a FIFO link
            pass
    
    def periodicalBroadcast(self):
        # There're two purposes of this periodical broadcast which acts as a heartbeat
        # 1) Let the followers know that the node is still alive
        # 2) Resend messages that might have been dropped due to network issue
        if self.currentRole == Role.Leader:
            for follower in self.nodes:
                self.replicateLog(self.nodeId, follower)
    
    def replicateLog(self, leaderId, followerId):
        # prefix: messages that the leader think its followers should have received
        # suffix: messages that the leader will send to followers 
        prefixLen = self.sentLength[followerId]
        suffix = [self.log[i] for i in range(prefixLen, len(self.log))]

        prefixTerm = 0
        if prefixLen > 0:
            prefixTerm = self.log[prefixLen-1].term
        
        # send msg to follower with followerId
        logRequest = LogRequest()
        self.sendMessageToNode(logRequest, leaderId, self.currentTerm, prefixLen, prefixTerm, self.commitLength, suffix)

    def onReceivingMessage(self, logResponse, follower, term, ack, success):
        if term == self.currentTerm and self.currentRole == Role.Leader:
            if success and ack >= self.ackedLength[follower]:
                self.sentLength[follower] = ack
                self.ackedLength[follower] = ack # why do we need both sentLength and ackedLength?
                self.commitLogEntries()
            elif self.sentLength[follower] > 0:
                # note that this is a simplified retry 
                self.sentLength[follower] = self.sentLength[follower] - 1
                self.replicateLog(self.nodeId, follower)
        elif term > self.currentTerm:
            self.currentTerm = term
            self.currentRole = Role.Follower
            self.votiedFor = None
            self.cancelElectionTime()
    
    def commitLogEntries(self):
        while self.commitLength < len(self.log):
            ack = 0
            for node in self.nodes:
                if self.ackedLength[node] > self.commitLength:
                    ack += 1
            if ack >= (len(self.nodes) + 1) // 2:
                # if majority of the followers have delivered the msg, the leader will deliver the message too 
                self.deliverMessage(self.log[self.commitLength])
                self.commitLength += 1
    
class LogRequest:
    pass