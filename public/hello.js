function Hello($scope, $http) {
    $scope.queryWord = "";
    $scope.ranker = "cosine";
    $scope.display = false;
    $scope.size = 10;
    $scope.start = 0;

    $scope.go = function() {
        $http.get('/search?query=' + $scope.queryWord + '&ranker=' + $scope.ranker
            + '&format=html&pageSize=' + $scope.size + '&pageStart=' + $scope.start).
            success(function(data) {
                $scope.documents = data;
                $scope.display = true;
                $scope.documents.forEach(function(doc) {
                    $http.get('/click?id=' + doc.id + '&query=' + doc.query + '&action=render');
                })
            });
    };

    $scope.clicklog = function(doc) {
        $http.get('/click?id=' + doc.id + '&query=' + doc.query + '&action=click').
        success(function() {
            alert("Doc #" + doc.id + "clicked record written to log");
        });
    };
}