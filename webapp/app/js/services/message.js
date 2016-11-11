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

KylinApp.service('MessageService', ['config_ui_messenger', function (config_ui_messenger,$log) {

  this.sendMsg = function (msg, type, actions, sticky, position) {
    var options = {
      'theme': config_ui_messenger.theme
    };

    var data = {
      message: msg,
      type: angular.isDefined(type) ? type : 'info',
      actions: actions,
      showCloseButton: true
    };

    // Whether sticky the message, otherwise it will hide after a period.
    if (angular.isDefined(sticky) && sticky === true) {
      data.hideAfter = false;
    }

    // Specify the position, otherwise it will be default 'bottom_right'.
    if (angular.isDefined(position) && config_ui_messenger.location.hasOwnProperty(position)) {
      options.extraClasses = config_ui_messenger.location[position];
    }

    Messenger(options).post(data);
  }
}]);

KylinApp.value('config_ui_messenger', {
  location: {
    top_left: 'messenger-fixed messenger-on-top messenger-on-left',
    top_center: 'messenger-fixed messenger-on-top',
    top_right: 'messenger-fixed messenger-on-top message-on-right',
    bottom_left: "messenger-fixed messenger-on-bottom messenger-on-left",
    bottom_center: 'messenger-fixed messenger-on-bottom',
    bottom_right: 'messenger-fixed messenger-on-bottom messenger-on-right'
  },
  theme: 'ice'
});
