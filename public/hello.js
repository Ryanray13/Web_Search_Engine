function Hello($scope, $http) {
	$http.get('http://localhost:25801/search?query=a&ranker=cosine&format=html').
	success(function(data) {
		$scope.documents = data;
	});
}