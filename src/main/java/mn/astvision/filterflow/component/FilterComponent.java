package mn.astvision.filterflow.component;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import mn.astvision.filterflow.model.enums.FilterLogicMode;

/**
 * @author zorigtbaatar
 */

@JsonDeserialize(using = FilterComponentDeserializer.class)
public abstract class FilterComponent {
    public abstract FilterLogicMode getLogic();
}

