function Hello($scope, $http) {
	$http.get('/search?query=a&ranker=cosine&format=html').
	success(function(data) {
		$scope.documents = data;
	});

    $scope.addlog = function(doc) {
        console.log(doc.id);
        $http.get('/click?id=' + doc.id + '&query=a' + '&action=click').
        success(function() {
            alert("Doc #" + doc.id + "clicked record written to log");
        });
    };
}