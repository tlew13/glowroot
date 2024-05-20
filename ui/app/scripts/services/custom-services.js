/* global glowroot*/
glowroot.factory('html2canvas', ['$window', '$q', function($window, $q) {
    // Check if html2canvas is loaded
    if (typeof html2canvas === 'undefined') {
        throw new Error('html2canvas is not loaded');
    }

    // Function to capture the element
    function capture(element, options) {
        // Use $q to create a promise around html2canvas functionality
        var deferred = $q.defer();
        $window.html2canvas(element, options).then(function(canvas) {
            deferred.resolve(canvas);
        }, function(error) {
            deferred.reject(error);
        });
        return deferred.promise;
    }

    // Return the capture function
    return {
        capture: capture
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