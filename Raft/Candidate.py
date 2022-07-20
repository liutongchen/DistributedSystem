# Learning notes from Distributed Systems 6.2 by Martin Kleppmann
from Role import Role

class Candidate:
    def __int__(self, nodeId, nodes):
        # The first four values need to stored on disk (stable storage)
        self.currentTerm = 0
        self.votedFor = None
        self.log = []
        self.commitLength = 0
        self.currentRole = Role.Candidate
        self.currentLeader = None
        self.votesReceived = set()
        self.sentLength = []
        self.ackedLength = []
        self.nodeId = nodeId
        self.nodes = nodes
    
    # invoke when node nodeId suspects leader has failed, or when election timeout
    def leaderFailure(self):
        self.currentTerm = self.currentTerm + 1
        self.currentRole = Role.Candidate
        self.votedFor = self.nodeId
        self.votesReceived = {self.nodeId}

        lastTerm = 0

        if len(self.log) > 0:
            lastTerm = self.log[-1].term
        
        voteRequest = voteRequest()
        message = (voteRequest, self.nodeId, self.currentTerm, len(self.log), self.lastTerm)

        for node in self.nodes:
            self.sendMessageToNode(node, message)
        
        self.startElectionTimer()
    
    def onReceivingVoteResponse(self, voteResponse, voterId, term, granted):
        if self.currentRole == Role.Candidate and term == self.currentTerm and granted:
            self.votesReceived.add(voterId)
            if len(self.votesReceived) >= (len(self.nodes) + 1) // 2:
                self.currentRole = Role.Leader
                self.currentLeader = self.nodeId
                self.cancelElectionTimer()
                for follower in self.nodes:
                    self.sentLength[follower] = len(self.log)
                    self.ackedLength[follower] = 0
                    self.replicateLog(self.nodeId, follower)
        elif term > self.currentTerm:
            self.currentTerm = term
            self.currentRole = Role.Follower
            self.votedFor = None
            self.cancelElectionTimer()
            