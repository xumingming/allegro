package io.github.xumingming.allegro;

import com.google.common.collect.ImmutableMap;

import java.beans.Transient;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static io.github.xumingming.allegro.Utils.checkState;
import static io.github.xumingming.allegro.Utils.isBlank;
import static io.github.xumingming.allegro.Utils.isNotBlank;
import static io.github.xumingming.allegro.Utils.mask;
import static java.lang.String.format;

/**
 * The config for running a suite of queries.
 */
public class SuiteConf
{
    private String runName;
    private String querySet;
    /**
     * Ip of the coordinator.
     */
    private String coordinatorIp;
    /**
     * Port of the coordinator.
     */
    private Integer coordinatorPort = 8080;

    /**
     * dbName to test
     */
    private String dbName;
    private String user;
    private String password;
    /**
     * The session properties to be set before executing the sql.
     */
    private String sessionProperties;
    /**
     * Whether to fetch the query's raw json.
     */
    private boolean fetchQueryDetail = true;

    public String getRunName()
    {
        return runName;
    }

    public void setRunName(String runName)
    {
        this.runName = runName;
    }

    public String getQuerySet()
    {
        return querySet;
    }

    public void setQuerySet(String querySet)
    {
        this.querySet = querySet;
    }

    public String getUser()
    {
        return user;
    }

    public void setUser(String user)
    {
        this.user = user;
    }

    public String getPassword()
    {
        return password;
    }

    public void setPassword(String password)
    {
        this.password = password;
    }

    public String getSessionProperties()
    {
        return sessionProperties;
    }

    public void setSessionProperties(String sessionProperties)
    {
        this.sessionProperties = sessionProperties;
    }

    @Transient
    public Map<String, String> getSessionPropertiesAsMap()
    {
        if (isBlank(sessionProperties)) {
            return ImmutableMap.of();
        }

        Map<String, String> ret = new HashMap<>();
        String[] parts = sessionProperties.split(",");
        for (String part : parts) {
            String[] keyAndValue = part.split("=");
            checkState(keyAndValue.length == 2, "Invalid session property: " + keyAndValue);
            ret.put(keyAndValue[0], keyAndValue[1]);
        }

        return ret;
    }

    public String getCoordinatorIp()
    {
        return coordinatorIp;
    }

    public void setCoordinatorIp(String coordinatorIp)
    {
        this.coordinatorIp = coordinatorIp;
    }

    public Integer getCoordinatorPort()
    {
        return coordinatorPort;
    }

    public void setCoordinatorPort(Integer coordinatorPort)
    {
        this.coordinatorPort = coordinatorPort;
    }

    public String getDbName()
    {
        return dbName;
    }

    public void setDbName(String dbName)
    {
        this.dbName = dbName;
    }

    public boolean isFetchQueryDetail()
    {
        return fetchQueryDetail;
    }

    public void setFetchQueryDetail(boolean fetchQueryDetail)
    {
        this.fetchQueryDetail = fetchQueryDetail;
    }

    @Transient
    public String getJdbcUrl(String source)
    {
        return format("jdbc:presto://%s:%s/%s?applicationNamePrefix=%s", coordinatorIp, coordinatorPort, dbName, source);
    }

    public boolean hasSessionProperties()
    {
        return isNotBlank(sessionProperties);
    }

    public String maskedPassword()
    {
        return mask(password);
    }

    public String validate()
    {
        if (isBlank(querySet)) {
            return "querySet is not specified!";
        }

        if (isBlank(coordinatorIp)) {
            return "coordinatorIp is not specified!";
        }

        if (isBlank(dbName)) {
            return "dbName is not specified!";
        }

        return null;
    }

    public String getTraceId(String queryName)
    {
        return format("%s_%s", runName, queryName).toLowerCase(Locale.ROOT);
    }

    @Override
    public String toString()
    {
        return "RunnerConf{" +
                "runName='" + runName + '\'' +
                ", querySet='" + querySet + '\'' +
                ", coordinatorIp='" + coordinatorIp + '\'' +
                ", coordinatorPort=" + coordinatorPort +
                ", dbName='" + dbName + '\'' +
                ", user='" + user + '\'' +
                ", password='" + password + '\'' +
                ", sessionProperties='" + sessionProperties + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SuiteConf that = (SuiteConf) o;
        return coordinatorPort == that.coordinatorPort
                && Objects.equals(runName, that.runName)
                && Objects.equals(querySet, that.querySet)
                && Objects.equals(coordinatorIp, that.coordinatorIp)
                && Objects.equals(dbName, that.dbName)
                && Objects.equals(user, that.user)
                && Objects.equals(password, that.password)
                && Objects.equals(sessionProperties, that.sessionProperties);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(runName, querySet, coordinatorIp, coordinatorPort, dbName, user, password, sessionProperties);
    }

    public SuiteConf cloneWithSensitiveInfoMasked()
    {
        SuiteConf cloned = new SuiteConf();
        cloned.setQuerySet(this.querySet);
        cloned.setRunName(this.runName);
        cloned.setCoordinatorIp(this.coordinatorIp);
        cloned.setCoordinatorPort(this.coordinatorPort);

        cloned.setDbName(this.dbName);
        cloned.setUser(this.user);
        cloned.setPassword(this.maskedPassword());

        cloned.setSessionProperties(this.sessionProperties);

        return cloned;
    }
}
