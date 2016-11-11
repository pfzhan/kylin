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

// TODO add cubes manager
KylinApp.service('cubesManager', function (CubeDescService,SweetAlert) {

    this.currentCube = {};

    this.cubeMetaFrame={};

    this.getCubeDesc = function(_cubeName){
      CubeDescService.query({cube_name: _cubeName}, {}, function (detail) {
        if (detail.length > 0&&detail[0].hasOwnProperty("name")) {
          return detail[0];
        }else{
          SweetAlert.swal('Oops...', "No cube detail info loaded.", 'error');
        }
      }, function (e) {
        if(e.data&& e.data.exception){
          var message =e.data.exception;
          var msg = !!(message) ? message : 'Failed to take action.';
          SweetAlert.swal('Oops...', msg, 'error');
        }else{
          SweetAlert.swal('Oops...', "Failed to take action.", 'error');
        }
      });

  }

})
