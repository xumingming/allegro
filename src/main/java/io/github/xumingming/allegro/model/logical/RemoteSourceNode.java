package io.github.xumingming.allegro.model.logical;

import java.util.List;
import java.util.stream.Collectors;

public class RemoteSourceNode
        extends PlanNode
{
    private List<String> remoteFragmentIds;

    public List<String> getRemoteFragmentIds()
    {
        return remoteFragmentIds;
    }

    public void setRemoteFragmentIds(List<String> remoteFragmentIds)
    {
        this.remoteFragmentIds = remoteFragmentIds;
    }

    public List<Integer> getRemoteFragmentIdsAsInt()
    {
        return remoteFragmentIds.stream().map(x -> Integer.parseInt(x)).collect(Collectors.toList());
    }
}
