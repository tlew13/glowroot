package org.glowroot.central.repo;

import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.glowroot.central.util.Session;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class overrides the default permissions for the roles.
 * It removes the fine grained permissions and replaces them with
 * application level permissions.  If you have access to an application,
 * then you have access to all its services and rollups.
 */
public class AccessRoleImpl implements IAccessRole {

    private final Session session;
    private final PreparedStatement readPS;
    private final Map<String, Set<String>> applicationRoles;

    /**
     * Constructor so we can reuse the session from RoleDao.
     * @param session The session to be used for database operations.
     * @throws Exception If any error occurs during the initialization.
     */
    public AccessRoleImpl(Session session) throws Exception {
        this.session = session;
        readPS = session.prepare("select app_name, global_role_prefix from application");
        applicationRoles = load();
    }

    // Default permissions for different roles
    private final String[] defaultAdminPermissions = {
            "agent:\"ROLLUPID::\":config:edit:alerts",
            "agent:\"ROLLUPID::\":config:edit:gauges",
            "agent:\"ROLLUPID::\":config:edit:general",
            "agent:\"ROLLUPID::\":config:edit:instrumentation",
            "agent:\"ROLLUPID::\":config:edit:jvm",
            "agent:\"ROLLUPID::\":config:edit:plugins",
            "agent:\"ROLLUPID::\":config:edit:syntheticMonitors",
            "agent:\"ROLLUPID::\":config:edit:transaction",
            "agent:\"ROLLUPID::\":config:edit:uiDefaults",
            "agent:\"ROLLUPID::\":config:view",
            "agent:\"ROLLUPID::\":error",
            "agent:\"ROLLUPID::\":incident",
            "agent:\"ROLLUPID::\":jvm",
            "agent:\"ROLLUPID::\":syntheticMonitor",
            "agent:\"ROLLUPID::\":transaction"};

    private final String[] defaultUserPermissions = {
            "agent:\"ROLLUPID::\":config:view",
            "agent:\"ROLLUPID::\":error",
            "agent:\"ROLLUPID::\":incident",
            "agent:\"ROLLUPID::\":jvm:environment",
            "agent:\"ROLLUPID::\":jvm:gauges",
            "agent:\"ROLLUPID::\":jvm:mbeanTree",
            "agent:\"ROLLUPID::\":jvm:systemProperties",
            "agent:\"ROLLUPID::\":syntheticMonitor",
            "agent:\"ROLLUPID::\":transaction"};

    private final String[] defaultAdministratorPermissions = {
            "agent:*:transaction",
            "agent:*:error",
            "agent:*:jvm",
            "agent:*:syntheticMonitor",
            "agent:*:incident",
            "agent:*:config",
            "admin"};


    /**
     * Returns the permissions for a given role.
     * @param roleName The name of the role.
     * @param dbPermissions The permissions stored in the database for the role.
     * @return A set of permissions for the role.
     */
    @Override
    public Set<String> getPermissionsForRole(String roleName, Set<String> dbPermissions){

        Set<String> roleApps = getDBPermissionApps(dbPermissions);
        String roleType = getAccessRoleType(roleName);
        Set<String> permissions = new HashSet<>();

        //Case 1 - Administrator - has all permissions
        //Case 2 - Administration - has admin permission
        //Case 3 - User/Admin - permissions based on apps in db, or if they are part of global role prefix
        //Case 3a - AllApp-User/Admin - has permissions on all apps
        //Case 3b - Must be a global-prefix type role (i.e. ALL-CSI-User/Admin)
        switch(roleType) {
            case "ADMINISTRATOR":
                permissions.addAll(Arrays.asList(defaultAdministratorPermissions));
                break;
            case "ADMINISTRATION":
                permissions.add("admin");
                break;
            case "USER":
            case "ADMIN":

                switch (roleName.toUpperCase()) {
                    case "ALLAPP-USER":
                        permissions.addAll(getDefaultUserPermissions("*"));
                        break;
                    case "ALLAPP-ADMIN":
                        permissions.addAll(getDefaultAdminPermissions("*"));
                        break;
                    default:
                        permissions.addAll(getGroupPermissionsForRole(roleType, roleName, roleApps));
                        break;
                }
                break;
        }

        return permissions;
    }

    /**
     * Returns the permissions for a given role.
     * Currently this will combine all apps found in the existing role database with any apps
     * found in the application table matching a global_role_prefix (if applicable)
     * @param roleType The role type, either USER or ADMIN.
     * @param roleName The full name of the role.
     * @param listedApps List of apps found associated with this role
     * @return A set of permissions for the role.
     */
    private Set<String> getGroupPermissionsForRole(String roleType, String roleName, Set<String> listedApps){

        Set<String> permissions = new HashSet<>();
        String baseName = getAppNameFromParts(roleName);
        try {
            Set<String> appsAllowed = new HashSet<>();
            appsAllowed.add(baseName);//add base name as standard permission set
            if (applicationRoles.containsKey(baseName)){
                appsAllowed.addAll(applicationRoles.get(baseName));
            }
            appsAllowed.addAll(listedApps);

            for (String app : appsAllowed) {
                if (roleType.equalsIgnoreCase("USER")){
                    permissions.addAll(getDefaultUserPermissions(app));
                } else { //else ADMIN
                    permissions.addAll(getDefaultAdminPermissions(app));
                }
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return permissions;
    }

    private Set<String> getDefaultUserPermissions(String appName){
        Set<String> permissions = new HashSet<>();
        for (String defaultPermission : defaultUserPermissions) {
            permissions.add(defaultPermission.replace("ROLLUPID", appName));
        }
        return permissions;
    }
    private Set<String> getDefaultAdminPermissions(String appName){
        Set<String> permissions = new HashSet<>();
        for (String defaultPermission : defaultAdminPermissions) {
            permissions.add(defaultPermission.replace("ROLLUPID", appName));
        }
        return permissions;
    }

    /**
     * Returns the applications associated with the permissions found in the existing role database
     * @param dbPermissions The permissions stored in the database for the role.
     * @return A set of applications associated with the permissions.
     */
    private Set<String> getDBPermissionApps(Set<String> dbPermissions) {
        Set<String> apps = new HashSet<>();

        for (String permission : dbPermissions) {
            permission = permission.trim(); // remove leading and trailing whitespaces

            String appName = extractApplicationName(permission);
            if (appName != null){
                String[] appNameParts = appName.split("-");
                if (appNameParts.length >= 2){
                    String baseName = appNameParts[0] + "-" + appNameParts[1];
                    apps.add(baseName);
                }
            }
        }
        return apps;
    }


    /**
     * Extracts the application name from the permission string.
     * Assumes all permission entries contain the AppName-Motsid-ServiceName surrounded in quotes and ends with ::
     * @param str
     * @return The AppName-Motsid-ServiceName string.
     */
    private static String extractApplicationName(String str) {
        //assumes pattern "AppName-Motsid-ServiceName::"
        Pattern pattern = Pattern.compile("\"(.*?)::\"");
        Matcher matcher = pattern.matcher(str);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return null;
    }

    private String getAccessRoleType(String role){
        if (role.toUpperCase().endsWith("-USER")) {
            return "USER";
        } else if (role.toUpperCase().endsWith("-ADMIN")) {
            return "ADMIN";
        } else if (role.equalsIgnoreCase("ADMINISTRATOR")) {
            return "ADMINISTRATOR";
        } else if (role.equalsIgnoreCase("ADMINISTRATION")) {
            return "ADMINISTRATION";
        }
        //The default will be USER if the role is not found
        //This seems to happen with test profiles that don't fit expected naming convention
        else {
            return "USER";
        }
    }

   private static String getAppNameFromParts(String input){
        String[] parts = input.split("-");
        if (parts.length >= 2){
            return parts[0] + "-" + parts[1].replace("::", "");
        }
        return "";
    }


    /**
     * Loads the global_role_prefix and applications from application table.
     * @return A map where the key is the global role prefix and the value is a set of application names.
     */
    private Map<String, Set<String>> load() {
        ResultSet results = session.read(readPS.bind());
        Map<String, Set<String>> applicationRoles = new HashMap<>();
        for (Row row : results) {
            String appName = row.getString("app_name");
            String globalRolePrefix = row.getString("global_role_prefix");
            if (globalRolePrefix == null) {
                continue; // skip this row if global_role_prefix is null
            }
            if (applicationRoles.containsKey(globalRolePrefix)){
                applicationRoles.get(globalRolePrefix).add(appName);
            } else {
                Set<String> roles = new HashSet<>();
                roles.add(appName);
                applicationRoles.put(globalRolePrefix, roles);
            }
        }
        return applicationRoles;
    }

}