package org.glowroot.ui;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.glowroot.common2.repo.ConfigRepository;
import org.glowroot.common2.repo.util.HttpClient;

import java.io.File;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;

@JsonService
public class UserPreferencesJsonService {

    private Properties props;
    private final HttpClient httpClient;

    public String accessToken;

    public long accessTokenExpires;

    public UserPreferencesJsonService (boolean central, List<File> confDirs,
                                       ConfigRepository configRepository,
                                       HttpClient httpClient, Properties props) throws Exception {
        this.props = props;
        this.httpClient = httpClient;
        this.accessToken = getAccessToken();
    }

    @GET(path = "/backend/admin/user-preferences/getFavorites", permission = "admin:edit:userPreferences")
    String getFavorites() throws Exception {
        if (this.accessTokenExpires < System.currentTimeMillis()){
            this.accessToken = getAccessToken();
        }
        String url = "http://localhost:3000/api/external/favorites";
        return this.httpClient.getWithAuthorizationHeader(url, "Bearer " + accessToken);
    }

    private String getAccessToken() throws Exception {
        String tenantId = this.props.getProperty("tenantId");
        String clientId = this.props.getProperty("clientId");
        String clientSecret = this.props.getProperty("clientSecret");
        String scope = this.props.getProperty("scope");
        String grantType = this.props.getProperty("grantType");
        String body = URLEncoder.encode("client_id", "UTF-8") + "=" + URLEncoder.encode(clientId, "UTF-8") + "&" +
                URLEncoder.encode("client_secret", "UTF-8") + '=' + URLEncoder.encode(clientSecret, "UTF-8") + "&" +
                URLEncoder.encode("scope", "UTF-8") + "=" + URLEncoder.encode(scope, "UTF-8") + "&" +
                URLEncoder.encode("grant_type", "UTF-8") + "=" + URLEncoder.encode(grantType, "UTF-8");
        String url = "https://login.microsoftonline.com/" + tenantId + "/oauth2/v2.0/token";
        String response = this.httpClient.post(url, body.getBytes(StandardCharsets.UTF_8), "application/x-www-form-urlencoded");
        String accessToken = null;
        ObjectMapper mapper = new ObjectMapper();
        AzureADToken azureADToken = mapper.readValue(response, new TypeReference<AzureADToken>() { });
        accessToken = azureADToken.getAccess_token();
        this.accessTokenExpires = System.currentTimeMillis() + azureADToken.getExpires_in()*1000;
        return accessToken;
    }

    private static class AzureADToken {
        private String token_type;
        private long expires_in;
        private long ext_expires_in;
        private String access_token;

        public String getToken_type() {
            return token_type;
        }

        public long getExpires_in() {
            return expires_in;
        }

        public long getExt_expires_in() {
            return ext_expires_in;
        }

        public String getAccess_token() {
            return access_token;
        }
    }
}
