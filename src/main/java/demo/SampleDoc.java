package demo;

import org.bson.codecs.pojo.annotations.BsonProperty;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown=true)
public class SampleDoc {
    @JsonProperty("_id")
    private String id;
    
    @JsonProperty("_rev")
    private String revision;

    @JsonProperty("ts")
    private String ts;

    @JsonProperty("value")
    private int value;

    @JsonProperty("Header")
    private Header header;
    
    @BsonProperty("_id")
    public String getId() {
        return id;
    }

    public void setId(String s) {
        id = s;
    }

    @BsonProperty("_rev")
    public String getRevision() {
        return revision;
    }

    public void setRevision(String s) {
        revision = s;
    }

    public void setDateTime(String s) {
        ts = s;
    }
    
    @BsonProperty("ts")
    public String getDateTime() {
        return ts;
    }

    public void setValue(int n) {
        value = n;
    }

    @BsonProperty("value")
    public int getValue() {
        return value;
    }

    @BsonProperty("Header")
    public Header getHeader() {
        return header;
    }

    public void setHeader(Header header) {
        this.header = header;
    }
}
