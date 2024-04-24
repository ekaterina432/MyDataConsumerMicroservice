package kafka.config;

import com.jcabi.xml.XML;
import lombok.RequiredArgsConstructor;

/**
 * парсер xml
 */
@RequiredArgsConstructor
public class TextXpath {
    private final XML xml;
    private final String node;

    @Override
    public String toString(){
        return this.xml.nodes(this.node)
                .get(0)
                .xpath("text()")
                .get(0);
    }
}
