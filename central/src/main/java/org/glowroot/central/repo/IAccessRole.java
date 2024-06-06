package org.glowroot.central.repo;

import java.util.Set;

public interface IAccessRole {

    public Set<String> getPermissionsForRole(String roleType, String rollupName);
    public String getAccessRoleType(String role);
    public String getApplicationName(String role);
}