namespace RaftConsensusOnAspnet.RaftConsensus.Core.Interfaces;

public interface IRaftNodeMetadataPersistentStorage
{
    public int CurrentTerm { get; set; }
    public (Guid VoteFor , int TermOfVote) VoteInfo { get; set; }


    public void RemoveExistingData();
}
