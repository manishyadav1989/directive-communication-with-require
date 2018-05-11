(function(angular){
		var myApp = angular.module('app', []);
		myApp.directive('tabs', function(){
			return {
				restrict : 'E',
				scope : {},
				transclude : true,
				controller : function(){
					this.tabs = [];
					this.addTab = function(tab){
						this.tabs.push(tab)
					}
					this.selectTab = function(index){
						for(let i=0; i<this.tabs.length; i++){
							this.tabs[i].selected = false;
						}
						this.tabs[index].selected = true;
					}
				},
				controllerAs: 'tabs',
				link : function(scope, elem, attr, ctrl){
					ctrl.selectTab(attr.active || 0);
				},
				template : `
					<div class="tabs">
						<ul class="tabs__list">
							<li ng-repeat="tab in tabs.tabs">
								<a href="" ng-bind="tab.label" ng-click="tabs.selectTab($index);"></a>
							</li>
						</ul>
						<div class="tabs_content" ng-transclude></div>
					</div>
				`
			}
		});
		
		myApp.directive('tab', function(){
			return{
				restrict:'E',
				scope:{
					label:'@'
				},
				require:'^tabs',
				transclude:true,
				template:`
					<div class="tabs_content" ng-if="tab.selected">
						<div ng-transclude></div>
					</div>
				`,
				link:function($scope, elem, attr, ctrl){
					$scope.tab = {
						label:$scope.label,
						selected:false
					}
					ctrl.addTab($scope.tab);
				}
			}
		});
	}(angular));