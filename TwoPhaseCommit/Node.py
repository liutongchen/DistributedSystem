# Two Phase Commit: a distributed algorithm in coordinating distributed atomic transtions.
                    # This algorithm utilizes the consensus algorithm Total Order Broadcast
                    
class Node:
    def __init__(self, nodeId):
        self.commitVotes = {}
        self.replicas = {}
        self.decided = {}
        self.nodeId = nodeId
    
    def initializeTransaction(self, T):
        # T represents transaction
        self.commitVotes[T] = {}
        self.replicas[T] = {}
        self.decided[T] = False

    def prepareCommit(self, T, R):
        # T: transaction; R: participating nodes
        for r in R:
            prepare = Prepare(r)
            self.send(prepare, T, R)
    
    def receivePrepare(self, T, R):
        self.replicas[T] = R # record all the nodes participating in the transaction
        ok = "is transaction T ready to commit on this replica node?"
        for r in self.replicas[T]:
            vote = Vote(r)
            self.totalOrderBroadcast(vote, T, self.nodeId, ok)

    # invoked when a node suspect node(replicaId) has crashed 
    def suspectNodeCrash(self, replicaId):
        for transaction in self.replicas:
            # for each transaction where the suspected node participates, do a total order broadcast to all the nodes in that transaction 
            if replicaId in transaction: 
                for node in transaction:
                    vote = Vote(node)
                    self.totalOrderBroadcast(vote, transaction, replicaId, False)
        
    def onDeliveringTotalOrderBroadcast(self, vote, T, replicaId, ok):
        if (not self.decided) and \
            (replicaId in self.replicas[T]) and \
            (replicaId not in self.commitVotes[T]):
                if ok:
                    self.commitVotes[T].add(replicaId)
                    if len(self.commitVotes[T]) == len(self.replicas[T]):
                        self.decided = True
                        self.commitTransaction(T)
                else:
                    self.decided = True
                    self.abortTransaction(T)
        
# dummy classes below
class Prepare:
    pass
class Vote:
    pass