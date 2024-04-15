/* global glowroot */

glowroot.factory('customService', function () {
    return {
        cluster: function () {
            var clusterinfo = [{
                'appname': 'TTRACE-12345',
                'clustername': 'PROD',
                'tapmurl': 'http://tapm-prod.web.att.com',
                'agenturl': 'http://tapm-prod-col.idns.cci.att.com:8181',
                'servicetype': 'other'
            },
            {
                'appname': 'TTRACE-INJECTOR',
                'clustername': 'PROD',
                'tapmurl': 'http://tapm-prod.web.att.com',
                'agenturl': 'http://tapm-prod-col.idns.cci.att.com:8181',
                'servicetype': 'other'
            }];

            return clusterinfo;
        }
    };
    /* $rootScope.getclusterdata =  function(){
     $http.get('backend/clusterinfo')
     .then(function (response) {
           $rootScope.clusterinfo = response.data;
     },function (response) {    	
       });     
     }; */

    /* $rootScope.getmainpage= function () {    	
              $http.get('backend/maintapmurl')    
        .then(function (response) {
            $rootScope.mainurl = response.data;
      
        },function (response) {
          });
    };
    
    $rootScope.mainpage= function () {
        window.location.href = $rootScope.mainurl.url;	
    }; */

    //ATTDEV end
}
);
