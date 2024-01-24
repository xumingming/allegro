package io.github.xumingming.allegro.model.enriched;

import io.github.xumingming.allegro.model.Stage;

import java.util.List;
import java.util.Map;

public class EnrichedStage
{
    private Stage stage;
    private EnrichedLogicalPlan logicalPlan;
    private List<EnrichedStage> subStages;

    public EnrichedStage(Stage stage, EnrichedLogicalPlan logicalPlan, List<EnrichedStage> subStages)
    {
        this.stage = stage;
        this.logicalPlan = logicalPlan;
        this.subStages = subStages;
    }

    public Stage getStage()
    {
        return stage;
    }

    public void setStage(Stage stage)
    {
        this.stage = stage;
    }

    public EnrichedLogicalPlan getLogicalPlan()
    {
        return logicalPlan;
    }

    public void setLogicalPlan(EnrichedLogicalPlan logicalPlan)
    {
        this.logicalPlan = logicalPlan;
    }

    public List<EnrichedStage> getSubStages()
    {
        return subStages;
    }

    public void setSubStages(List<EnrichedStage> subStages)
    {
        this.subStages = subStages;
    }

    public Map<String, EnrichedPlanNode> getPlanNodesAsMap()
    {
        return logicalPlan.getPlanNodesAsMap();
    }
}
