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
KylinApp.service("LicenseService", function (kylinCommon,KapSystemService){
   var _this = this;
   this.license = {};
   this.init = function () {
     KapSystemService.license({},function(data){
       if(!data['kap.version']){
         data['kap.version'] = 'N/A';
       }
       if(!data['kap.license.statement']){
         data['kap.license.statement'] = 'N/A';
       }
       if(!data['kap.dates']){
         data['kap.dates'] = 'N/A';
       }
       if(!data['kap.commit']){
         data['kap.commit'] = 'N/A';
       }
       if(!data['kylin.commit']){
          data['kylin.commit'] = 'N/A';
        }
        if(!data['kap.license.isEvaluation']){
          data['kap.license.isEvaluation'] = "false";
        }
        if(!data['kap.license.serviceEnd']){
          data['kap.license.serviceEnd'] = "N/A";
        }
        if(!data['kylin.commit']){
          data['kylin.commit'] = 'N/A';
        }
       if(!data['kap.kyaccount.username']){
         data['kap.kyaccount.username'] = 'N/A';
       }
        _this.license = data;
        },function(e){
          kylinCommon.error_default(e);
        })
    }
});
