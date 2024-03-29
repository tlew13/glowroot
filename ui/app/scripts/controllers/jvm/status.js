/*
 * Copyright 2013-2023 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/* global glowroot */

glowroot.controller('JvmStatusCtrl', [
  '$scope',
  '$http',
  'httpErrors',
  function ($scope, $http, httpErrors) {

    $scope.$parent.heading = 'Status';

    if ($scope.hideMainContent()) {
      return;
    }
        $http.get('/backend/jvm/status?agent-id=' + encodeURIComponent($scope.agentId))
            .then(function (response) {
                $scope.loaded = true;
                $scope.live = response.data.live;
            }, function (response) {
                httpErrors.handle(response);
            });
  }
]);
