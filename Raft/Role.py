import enum

class Role(enum.Enum):
    Follower = "Follower"
    Candidate = "Candidate"
    Leader = "Leader"