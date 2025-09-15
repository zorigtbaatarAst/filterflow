package mn.astvision.filterflow.component;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

/**
 * @author zorigtbaatar
 */

@Component
public class FilterContextHolder implements ApplicationContextAware {
    private static final Logger log = LoggerFactory.getLogger(FilterContextHolder.class);
    private static ApplicationContext context;

    public static <T> T getBean(Class<T> requiredType) {
        ensureContext();
        return context.getBean(requiredType);
    }

    public static Object getBean(String name) {
        ensureContext();
        return context.getBean(name);
    }

    public static <T> T getBean(String name, Class<T> requiredType) {
        ensureContext();
        return context.getBean(name, requiredType);
    }

    public static boolean containsBean(String name) {
        return context != null && context.containsBean(name);
    }

    public static ApplicationContext getApplicationContext() {
        ensureContext();
        return context;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        if (FilterContextHolder.context == null) {
            FilterContextHolder.context = applicationContext;
            log.info("✅ SpringContextHolder initialized.");
        } else {
            log.warn("⚠️ SpringContextHolder was already initialized. Ignoring reinitialization.");
        }
    }

    private static void ensureContext() {
        if (context == null) {
            throw new IllegalStateException("Spring context is not initialized yet.");
        }
    }
}
