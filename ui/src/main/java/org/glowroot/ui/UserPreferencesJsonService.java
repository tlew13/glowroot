package org.glowroot.ui;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.glowroot.common2.repo.ConfigRepository;
import org.glowroot.common2.repo.util.HttpClient;

import java.io.File;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;

@JsonService
public class UserPreferencesJsonService {

    private final HttpClient httpClient;

    public String accessToken;

    public long accessTokenExpires;

    public UserPreferencesJsonService (boolean central, List<File> confDirs,
                                       ConfigRepository configRepository, HttpClient httpClient) throws Exception {
        this.httpClient = httpClient;
        this.accessToken = getAccessToken();
    }

    @GET(path = "/backend/admin/user-preferences/getFavorites", permission = "admin:edit:userPreferences")
    String getFavorites() throws Exception {
        if (this.accessTokenExpires < System.currentTimeMillis()){
            this.accessToken = getAccessToken();
        }
        System.out.println(this.accessTokenExpires);
        String url = "http://localhost:3000/api/external/favorites";
        return this.httpClient.getWithAuthorizationHeader(url, "Bearer " + accessToken);
    }

    private String getAccessToken() throws Exception {
        String tenantId = "e741d71c-c6b6-47b0-803c-0f3b32b07556";
        String clientId = "b20d9b6e-1b0c-422b-892c-7b0aa65ffa58";
        String clientSecret = "UTe8Q~uC~v-VYuHozWsOJQUWNOFkPwGTJ3QTjaDw";
        String scope = "api://next-tapm-ui-auth.att.com/.default";
        String grantType = "client_credentials";
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
