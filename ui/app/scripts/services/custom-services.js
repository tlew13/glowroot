/* global glowroot*/

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