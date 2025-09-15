package mn.astvision.filterflow.builders.base;

import mn.astvision.filterflow.model.FilterOptions;

public interface IOptionFlow<SELF> {
    FilterOptions getOptions();


    @SuppressWarnings("unchecked")
    default SELF failFast(boolean enabled) {
        getOptions().setFailFast(enabled);
        return (SELF) this;
    }

    @SuppressWarnings("unchecked")
    default SELF parallel() {
        getOptions().enableParallel();
        return (SELF) this;
    }

    @SuppressWarnings("unchecked")
    default SELF resolveVirtualFields() {
        getOptions().setResolveVF(true);
        return (SELF) this;
    }

    @SuppressWarnings("unchecked")
    default SELF debug(boolean enabled) {
        getOptions().setDebug(enabled);
        return (SELF) this;
    }
}
