/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

KylinApp.service('ProjectModel', function () {

  this.projects = [];
  this.selectedProject = "_null";


  this.setSelectedProject = function (project) {
    var _projects =[];
    angular.forEach(this.projects,function(pro){
      _projects.push(pro.name);
    })
    if (_projects.indexOf(project) > -1 || !project) {
      this.selectedProject = project;
    }
  };
  this.getSelectedProject = function (project) {
    if (this.selectedProject == "_null") {
      return null;
    }
    return this.selectedProject;
  };

  this.isSelectedProjectValid = function(){
    if(this.selectedProject == "_null"){
      return false;
    }
    return true;
  }

  this.setProjects = function (projects) {
    if (projects.length) {
      this.projects = projects;
    }
  }

  this.addProject = function (project) {
    this.projects.push(project);
    this.sortProjects();
  }

  this.removeProject = function (project) {
    var index = -1;
    for (var i = 0; i < this.projects.length; i++) {
      if (this.projects[i].name == project) {
        index = i;
        break;
      }
    }
    if (index > -1) {
      this.projects.splice(index, 1);
    }
    this.selectedProject = this.projects[0];
    this.sortProjects();
  }

  this.updateProject = function (_new, _old) {
    for (var i = 0; i < this.projects.length; i++) {
      if (this.projects[i].name === _old) {
        this.projects[i].name = _new;
        break;
      }
    }
  }

  this.getProjects = function () {
    return this.projects;
  }

  this.getProjectByCubeModel = function (modelName) {
    for (var i = 0; i < this.projects.length; i++) {
      if (!this.projects[i].models) {
        continue;
      }
      for (var j = 0; j < this.projects[i].models.length; j++) {
        var model = this.projects[i].models[j];
        if (model.toUpperCase() === modelName.toUpperCase()) {
          return this.projects[i].name;
        }
      }
    }
    ;
    return this.getSelectedProject();
  }

  this.sortProjects = function () {
    this.projects = _.sortBy(this.projects, function (i) {
      return i.name.toLowerCase();
    });
  }

  this.clear = function(){
    this.projects = [];
    this.selectedProject = "_null";
  }

  this.clearProjects = function(){
    this.projects = [];
  }

  this.getIndex = function(project){
    var index = -1;
    for (var i = 0; i < this.projects.length; i++) {
      if (this.projects[i].name == project) {
        index = i;
        break;
      }
    }
    return index;

  }

})
