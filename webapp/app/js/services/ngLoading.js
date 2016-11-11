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

var module = angular.module('ngLoadingRequest', []);
module.provider('loadingRequest', function () {

  this.$get = ['$document', '$window', function ($document, $window) {
    var body = $document.find('body');

    var loadTemplate = angular.element('<div class="kylinLoadingRequest"><div class="loadingOverlay" ></div>' +
    '<div id="loadingCntnr" class="showbox" style="opacity: 0; margin-top: 250px;">' +
    '<div class="loadingWord" ><img src="image/waiting.gif"><span>Please wait...</span></div>' +
    '</div> </div>');

    var createOverlay = function () {
      if (!body.find(".kylinLoadingRequest").length) {
        body.append(loadTemplate);
      }
      $(".loadingOverlay").css({'display': 'block', 'opacity': '0.8'});
      $(".showbox").stop(true).animate({'margin-top': '300px', 'opacity': '1'}, 200);
    };
    return {
      show: function () {
        createOverlay();
      },
      hide: function () {
        $(".showbox").stop(true).animate({'margin-top': '250px', 'opacity': '0'}, 2000);
        $(".loadingOverlay").css({'display': 'none', 'opacity': '0'});
        if (body.find(".kylinLoadingRequest").length) {
          body.find(".kylinLoadingRequest").remove();
        }

      }
    }

  }]
});
