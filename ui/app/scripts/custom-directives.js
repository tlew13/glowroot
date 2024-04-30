/* global glowroot */

glowroot.directive('popupDialog', ['popupService',  function(popupService) {
    return {
      restrict: 'E',
       link: function(scope, element, attrs) {
           scope.popupService = popupService;
       },
       template: '<div class="overlay" ng-if="popupService.isDialogVisible" ng-click="popupService.hideDialog()" style="position: fixed; top: 0; left: 0; width: 100%; height: 100%; background-color: rgba(0, 0, 0, 0.2); z-index: 999;"><div class="popup-dialog" style="position: fixed; top: 50%; left: 50%; transform: translate(-50%, -50%); background-color: #fff; border: 0px solid #000; border-radius: 10px; padding: 20px; z-index: 1000; text-align: center" ng-if="popupService.isDialogVisible" ng-click="$event.stopPropagation();"><div>{{popupService.message}}</div><br><button class="btn-primary" ng-click="popupService.hideDialog()">OK</button></div></div>'
    };
  }]);