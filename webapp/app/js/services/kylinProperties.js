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

KylinApp.service('kylinConfig', function (AdminService, $log) {
  var _config;
  var timezone;
  var deployEnv;
  var profile;


  this.init = function () {
    return AdminService.publicConfig({}, function (config) {
      _config = config.config;
    }, function (e) {
      $log.error("failed to load kylin.properties" + e);
    });
  };

  this.getProperty = function (name) {
    var result=(new RegExp(name+"=(.*?)\\n")).exec(_config);
    return result&&result[1]||"";
  }

  this.getTimeZone = function () {
    if (!this.timezone) {
      this.timezone = this.getProperty("kylin.web.timezone").trim();
    }
    return this.timezone;
  }

  this.getProfile = function () {
    if (!this.profile) {
      this.profile = this.getProperty("kylin.security.profile").trim();
    }
    return this.profile;
  }

  this.isCacheEnabled = function(){
    var status = this.getProperty("kylin.query.cache-enabled").trim();
    if(status!=='false'){
      return true;
    }
    return false;
  }

  //deprecated
  this.getDeployEnv = function () {
    this.deployEnv = this.getProperty("kylin.env");
    if (!this.deployEnv) {
      return "DEV";
    }
    return this.deployEnv.toUpperCase().trim();
  }

  this.getHiveLimit = function () {
    this.hiveLimit = this.getProperty("kylin.web.hive-limit");
    if (!this.hiveLimit) {
      return 20;
    }
    return this.hiveLimit;
  }

  this.getStorageEng = function () {
    this.StorageEng = this.getProperty("kylin.storage.default").trim();
      if (!this.StorageEng) {
        return 2;
      }
      return this.StorageEng;
    }

  this.getCubeEng = function () {
    this.CubeEng = this.getProperty("kylin.engine.default").trim();
    if (!this.CubeEng) {
      return 2;
    }
      return this.CubeEng;
  }
  //fill config info for Config from backend
  this.initWebConfigInfo = function () {

    try {
      Config.reference_links.hadoop.link = this.getProperty("kylin.web.link-hadoop").trim();
      Config.reference_links.diagnostic.link = this.getProperty("kylin.web.link-diagnostic").trim();
      Config.contact_mail = this.getProperty("kylin.web.contact-mail").trim();
      var doc_length = this.getProperty("kylin.web.help.length").trim();
      for (var i = 0; i < doc_length; i++) {
        var _doc = {};
        _doc.name = this.getProperty("kylin.web.help." + i).trim().split("|")[0];
        _doc.displayName = this.getProperty("kylin.web.help." + i).trim().split("|")[1];
        _doc.link = this.getProperty("kylin.web.help." + i).trim().split("|")[2];
        Config.documents.push(_doc);
      }
    } catch (e) {
      $log.error("failed to load kylin web info");
    }
  }

});

