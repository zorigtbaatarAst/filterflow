package mn.astvision.filterflow.builders.base;

import mn.astvision.filterflow.component.factory.FilterExecutorFactory;
import mn.astvision.filterflow.model.FilterGroup;
import mn.astvision.filterflow.model.FilterOptions;

public interface IFilterContext<T> {
    Class<T> getTargetType();

    FilterGroup getFilterGroup();

    FilterOptions getOptions();

    FilterExecutorFactory getFactory();

}
