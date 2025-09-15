package mn.astvision.filterflow.model;

import lombok.Data;
import lombok.experimental.FieldNameConstants;

@Data
@FieldNameConstants
public class DbExplainOptions {
    private boolean enabled; // master switch
    private boolean verbose; // full JSON
    private boolean timing; // execution time
    private boolean scanStats; // docs/keys scanned
    private boolean indexStats; // index used
    private boolean queryPlannerStats; // query planner
    private boolean allPlans; // all candidate plans
    private boolean planCacheStats; // plan cache stats
    private boolean planCacheHistograms; // plan cache histograms

    private boolean scanIndexes;

    public static DbExplainOptions byDefault() {
        return new DbExplainOptions();
    }

    public boolean dbExplainEnabled() {
        return isEnabled()
                || isVerbose()
                || isTiming()
                || isScanStats()
                || isIndexStats()
                || isQueryPlannerStats()
                || isAllPlans();
    }

    public String getVerbosity() {
        if (allPlans) return "allPlansExecution";
        if (timing || scanStats) return "executionStats"; // any exec stats requested
        return "queryPlanner"; // default
    }

}
