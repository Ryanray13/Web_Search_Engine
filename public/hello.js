function Hello($scope, $http) {
    $scope.queryWord = "";
    $scope.ranker = "cosine";
    $scope.display = false;

    $scope.go = function() {
        $http.get('/search?query=' + $scope.queryWord + '&ranker=' + $scope.ranker + '&format=html').
            success(function(data) {
                $scope.documents = data;
                $scope.display = true;
            });
    };

    $scope.addlog = function(doc) {
        console.log(doc.id);
        $http.get('/click?id=' + doc.id + '&query=a' + '&action=click').
        success(function() {
            alert("Doc #" + doc.id + "clicked record written to log");
        });
    };
}