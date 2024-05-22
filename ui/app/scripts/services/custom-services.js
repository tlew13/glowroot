/* global glowroot*/
glowroot.factory('html2canvas', ['$window', function($window) {
    if (typeof html2canvas === 'undefined') {
        throw new Error('html2canvas is not loaded');
    }
    return {
        capture: function(element, options) {
            return $window.html2canvas(element, options);
        }
    };
}]);

glowroot.factory('popupService', function() {
    var service = {
        isDialogVisible: false,
        message: '',
        showDialog: function(message) {
            service.isDialogVisible = true;
            service.message = message;
        },
        hideDialog: function() {
            service.isDialogVisible = false;
        }
    };
    return service;
});

glowroot.factory('addFavoriteService', ['$http', 'popupService', function($http, popupService) {
   var service = {
      postData: function(agentId, cardType) {
         if (agentId === null){
            popupService.showDialog('Error saving favorite. Please try again later.');
         }
         var data = {
           agentId: agentId,
           cardType: cardType
         };
         $http.post('backend/admin/user-preferences/addFavorite', data)
           .then(function(response) {
             popupService.showDialog(response.data);
           },
           function(response) {
             popupService.showDialog('Error saving favorite. Please try again later. ' + response.statusText);
           });
      }
   };
   return service;
}]);

glowroot.factory('addScreenshotService', ['$http', 'popupService', function($http, popupService) {
   var service = {
      postData: function(imageURL, notebookUniqueIdentifier, deepLink) {
         if (imageURL === null){
            popupService.showDialog('Error saving screenshot. Please try again later.');
         }
         var data = {
           imageURL: imageURL,
           notebookUniqueIdentifier: notebookUniqueIdentifier,
           deepLink: deepLink
         };
         $http.post('backend/admin/user-screenshots/addScreenshot', data)
           .then(function(response) {
             popupService.showDialog(response.data);
           },
           function(response) {
             popupService.showDialog('Error saving screenshot. Please try again later. ' + response.statusText);
           });
      }
   };
   return service;
}]);