/**
 * Created by luguosheng on 17/2/13.
 */
KylinApp.factory('KybotDiagnosisService', ['$resource', function ($resource) {
  return $resource(Config.service.url + 'kybot/:action', {}, {
    uploadPackage: {method: 'GET', params: {action:'upload'}, isArray: false},
    downPackage: {method: 'GET', params: {action:'dump'}, isArray: false}
  });
}]);
