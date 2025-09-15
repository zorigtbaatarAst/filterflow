package mn.astvision.filterflow.builders.base;

import mn.astvision.filterflow.component.executors.FilterExecutor;

public class DefaultFilterFlowBuilder<T> extends BaseFilterFlowBuilder<T, DefaultFilterFlowBuilder<T>> {
    public DefaultFilterFlowBuilder(FilterExecutor.Builder<T> builder) {
        super(builder);
    }
}
