/**
 * Created by feiguan on 12/13/14.
 */

var app = angular.module('myApp', []);

function appCtrl($scope, $http) {
    $scope.haveResults = false;
    $scope.haveKnowledge = false;
    $scope.haveSpellcheck = false;
    $scope.haveSearchResults = false;
    $scope.knowledgeMore = false;
    $scope.queryWord = "";
    $scope.ranker = "favorite";
    $scope.totalItems = 10;
    $scope.currentPage = 1;
    $scope.start = 0;

    $scope.onKeyPress = function ($event) {
        if ($event.keyCode == 13 && $scope.queryWord != "") {
            $scope.go(true, 1, true);
        }
    };

    $scope.getNumber = function(num) {
        var array = [];
        if ($scope.currentPage < 6) {
            for (var i = 1; i <= 10; i++)
                array.push(i);
            return array;
        } else  {
            for (var i = $scope.currentPage - 4; i <= $scope.currentPage + 5; i++)
                array.push(i);
            return array;
        }
    }

    $scope.go = function(spellcheck, pageNum, know) {
        $http.get('/search?query=' + $scope.queryWord + '&ranker=' + $scope.ranker
        + '&format=html&numdocs=10&spellcheck=' + spellcheck.toString() + '&know='
        + know.toString() + '&page=' + pageNum).
            success(function(data) {
                $scope.currentPage = pageNum;
                $scope.haveResults = true;
                var docs = data.results;
                var docus = [];
                docs.forEach(function (ele, idx, arr) {
                    var docu = {};
                    docu.url = ele.url;
                    docu.title = decodeURIComponent(ele.title).replace(/\+/g,' ');
                    docu.snippet = decodeURIComponent(ele.snippet).replace(/\+/g,' ');
                    docu.filePath = ele.filePath;
                    docus.push(docu);
                });
                $scope.documents = docus;
                $scope.haveSearchResults = docus.length != 0;

                $scope.haveKnowledge = data.knowledge != null;
                if ($scope.haveKnowledge) {
                    var know = {};
                    know.title = decodeURIComponent(data.knowledge.title).replace(/\+/g, ' ');
                    know.url = data.knowledge.url;
                    know.knowledge = decodeURIComponent(data.knowledge.knowledge).replace(/\+/g, ' ');
                    if (know.knowledge.length > 300) {
                        know.short = decodeURIComponent(data.knowledge.knowledge).replace(/\+/g, ' ').substring(0, 300);
                        $scope.knowledgeMore = true;
                    }
                    know.vote = data.knowledge.vote;
                    $scope.knowledge = know;
                }

                $scope.haveSpellcheck = data.spellcheck != null;
                if ($scope.haveSpellcheck) {
                    $scope.spellcheck = decodeURIComponent(data.spellcheck).replace(/\+/g, ' ');
                    $scope.correctSpell = function() {
                        $scope.queryWord = $scope.spellcheck;
                        $scope.go(false, 1, true);
                    }
                }
            });
    };

    $scope.showMore = function () {
        $scope.knowledgeMore = false;
    };
    
    $scope.showLess = function () {
        $scope.knowledgeMore = true;
    };

    $scope.pageChanged = function (pageNum) {
        if (pageNum == 1)
            $scope.go(true, 1, true);
        else
            $scope.go(false, pageNum, false);
    }
}