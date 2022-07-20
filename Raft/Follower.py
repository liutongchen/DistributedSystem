# Learning notes from Distributed Systems 6.2 by Martin Kleppmann
from re import A
from Role import Role

class Follower:
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

    def recoveryFromCrash(self):
        self.currentRole = Role.Follower
        self.currentLeader = None
        self.votesReceived = set()
        self.sentLength = []
        self.ackedLength = []

    # invoke when receiving vote request from another node
    def voteOnNewLeader(self, voteRequest, candidateId, candidateTerm, candidateLogLength, candidateLogTerm):
        if candidateTerm > self.currentTerm:
            self.currentTerm = candidateTerm
            self.currentRole = Role.Follower
            self.votedFor = None

        lastTerm = 0
        if len(self.log) > 0:
            lastTerm = self.log[-1].term
        logOk = (candidateLogTerm > lastTerm) or (candidateLogTerm == lastTerm and candidateLogLength >= len(self.log))

        if candidateTerm == self.currentTerm and logOk and self.votedFor in {candidateId, None}:
            self.votedFor = candidateId
            voteResponse = VoteResponse()
            self.sendMessageToNode(voteResponse, self.nodeId, self.currentTerm, True) # send msg to candidate node 
        else:
            voteResponse = VoteResponse()
            self.sendMessageToNode(voteResponse, self.nodeId, self.currentTerm, False) # send msg to candidate node 
    
    def receiveMessage(self, logReq, leaderId, term, prefixLen, prefixTerm, leaderCommit, suffix):
        if term > self.currentTerm:
            self.currentTerm = term
            self.votedFor = None
            self.cancelElectionTimer()
        
        if term == self.currentTerm:
            self.currentRole = Role.Follower
            self.currentLeader = leaderId

        logOk = (len(self.log) >= prefixLen) and (prefixLen == 0 or self.log[prefixLen-1].term == prefixTerm)

        if term == self.currentTerm and logOk:
            self.appendEntries(prefixLen, leaderCommit, suffix) 
            ack = prefixLen + len(suffix)

            # letting leader know that the message has been delivered
            logResponse = LogResponse()
            self.sendMessageToNode(logResponse, self.nodeId, self.currentTerm, ack, True)
        else:
            logResponse = LogResponse()
            self.sendMessageToNode(logResponse, self.nodeId, self.currentTerm, 0, False)
        
    def appendEntries(self, prefixLen, leaderCommit, suffix):
        if len(suffix) > 0 and len(self.log) > prefixLen:
            # current node has more messages delivered than the leader expected
            idx = min(len(self.log), prefixLen + len(suffix)) - 1 
            if self.log[idx].term != suffix[idx-prefixLen].term:
                # truncate follower's log
                self.log = [log[i] for i in range(prefixLen)]

        if prefixLen + len(suffix) > len(self.log):
            for i in range(len(self.log) - prefixLen, len(suffix)):
                self.log.append(suffix[i])
        
        if leaderCommit > self.commitLength:
            for i in range(self.commitLength, leaderCommit):
                # deliver message in buffer to application
                self.deliverMessageToApp(self.log[i])
    

class VoteRequest:
    pass

class VoteResponse:
    pass

class LogResponse:
    pass

