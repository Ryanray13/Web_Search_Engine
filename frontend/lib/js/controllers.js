/**
 * Created by feiguan on 12/13/14.
 */

var app = angular.module('myApp', []);

function appCtrl($scope, $http) {
    $scope.haveResults = false;
    $scope.haveKnowledge = false;
    $scope.haveSpellcheck = false;
    $scope.queryWord = "";
    $scope.ranker = "favorite";
    $scope.size = 10;
    $scope.start = 0;

    $scope.go = function() {
        $http.get('/search?query=' + $scope.queryWord + '&ranker=' + $scope.ranker
        + '&format=html&numdocs=10').
            success(function(data) {
                $scope.haveResults = true;
                var docs = data.results;
                var docus = [];
                docs.forEach(function (ele, idx, arr) {
                    var docu = {};
                    docu.url = ele.url;
                    docu.title = decodeURIComponent(ele.title).replace(/\+/g,' ');
                    docus.push(docu);
                });
                $scope.documents = docus;

                $scope.haveKnowledge = data.knowledge != null;
                if ($scope.haveKnowledge) {
                    var know = {};
                    know.title = decodeURIComponent(data.knowledge.title).replace(/\+/g, ' ');
                    know.url = data.knowledge.url;
                    know.knowledge = decodeURIComponent(data.knowledge.knowledge).replace(/\+/g, ' ');
                    know.vote = data.knowledge.vote;
                    $scope.knowledge = know;
                }

                $scope.haveSpellcheck = data.spellcheck != null;
                if ($scope.haveSpellcheck) {
                    $scope.spellcheck = decodeURIComponent(data.spellcheck).replace(/\+/g, ' ');
                    $scope.correctSpell = function() {
                        $scope.queryWord = $scope.spellcheck;
                        $scope.go();
                    }
                }
            });
    };
}