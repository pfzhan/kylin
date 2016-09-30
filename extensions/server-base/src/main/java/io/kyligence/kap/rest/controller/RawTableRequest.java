package io.kyligence.kap.rest.controller;

public class RawTableRequest {
    private String uuid;
    private String rawTableName;
    private String rawTableDescData;
    private boolean successful;
    private String message;
    private String project;

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    /**
     * @return the message
     */
    public String getMessage() {
        return message;
    }

    /**
     * @param message
     *            the message to set
     */
    public void setMessage(String message) {
        this.message = message;
    }

    /**
     * @return the status
     */
    public boolean getSuccessful() {
        return successful;
    }

    /**
     * @param status
     *            the status to set
     */
    public void setSuccessful(boolean status) {
        this.successful = status;
    }

    public RawTableRequest() {
    }

    public RawTableRequest(String rawTableName, String rawTableDescData) {
        this.rawTableName = rawTableName;
        this.rawTableDescData = rawTableDescData;
    }

    public String getRawTableDescData() {
        return this.rawTableDescData;
    }

    public void setRawTableDescData(String rawTableDescData) {
        this.rawTableDescData = rawTableDescData;
    }

    /**
     * @return the cubeDescName
     */
    public String getRawTableName() {
        return this.rawTableName;
    }

    /**
     * @param rawTableName
     *            the cubeDescName to set
     */
    public void setRawTableName(String rawTableName) {
        this.rawTableName = rawTableName;
    }

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }
}
