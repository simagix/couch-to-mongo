package demo;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;

@JsonIgnoreProperties
public class SampleDoc {
    private String id;
    private String revision;
    private String ts;
    private int value;
    
    @JsonProperty("_id")
    public String getId() {
        return id;
    }

    @JsonProperty("_id")
    public void setId(String s) {
        id = s;
    }

    @JsonProperty("_rev")
    public String getRevision() {
        return revision;
    }

    @JsonProperty("_rev")
    public void setRevision(String s) {
        revision = s;
    }

    @JsonProperty("ts")
    public void setDateString(String s) {
        ts = s;
    }
    
    @JsonProperty("ts")
    public String getDateString() {
        return ts;
    }

    @JsonProperty("value")
    public void setValue(int n) {
        value = n;
    }

    @JsonProperty("value")
    public int getValue() {
        return value;
    }
}
