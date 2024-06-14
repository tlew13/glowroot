package org.glowroot.central.repo;

import java.util.Set;

public interface IAccessRole {

    public Set<String> getPermissionsForRole(String roleName, Set<String> dbPermissions);

}