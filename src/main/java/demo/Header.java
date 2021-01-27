package demo;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;

@JsonIgnoreProperties
public class Header {
    @JsonProperty("SchemaVersion")
    private String schemaVersion;
    @JsonProperty("DocumentType")
    private String documentType;
    @JsonProperty("SourceSystemIdentifier")
    private String sourceSystemIdentifier;
    @JsonProperty("SubSystemIdentifier")
    private String subSystemIdentifier;
    @JsonProperty("DocumentSequenceNumber")
    private String documentSequenceNumber;
    @JsonProperty("SourceSystemPrimaryKey")
    private String sourceSystemPrimaryKey;
    @JsonProperty("JobIdentifier")
    private String jobIdentifier;
    @JsonProperty("EventDateTime")
    private String eventDateTime;
    @JsonProperty("EventType")
    private String eventType;
    @JsonProperty("BuildVersion")
    private String buildVersion;

    public String getSchemaVersion() {
        return schemaVersion;
    }

    public void setSchemaVersion(String s) {
        schemaVersion = s;
    }

    public String getDocumentType() {
        return documentType;
    }

    public void setDocumentType(String s) {
        documentType = s;
    }

    public String getSourceSystemIdentifier() {
        return sourceSystemIdentifier;
    }

    public void setSourceSystemIdentifier(String s) {
        sourceSystemIdentifier = s;
    }

    public String getSubSystemIdentifier() {
        return subSystemIdentifier;
    }

    public void setSubSystemIdentifier(String s) {
        subSystemIdentifier = s;
    }

    public String getDocumentSequenceNumber() {
        return documentSequenceNumber;
    }

    public void setDocumentSequenceNumber(String s) {
        documentSequenceNumber = s;
    }

    public String getSourceSystemPrimaryKey() {
        return sourceSystemPrimaryKey;
    }

    public void setSourceSystemPrimaryKey(String s) {
        sourceSystemPrimaryKey = s;
    }

    public String getJobIdentifier() {
        return jobIdentifier;
    }

    public void setJobIdentifier(String s) {
        jobIdentifier = s;
    }

    public String getEventDateTime() {
        return eventDateTime;
    }

    public void setEventDateTime(String s) {
        eventDateTime = s;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String s) {
        eventType = s;
    }

    public String getBuildVersion() {
        return buildVersion;
    }

    public void setBuildVersion(String s) {
        buildVersion = s;
    }
}
