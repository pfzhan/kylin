<template>
  <div class="start_kybot">
	  	<p><a href="https://kybot.io" target="_blank" class="blue">KyBot</a> {{$t('protocol')}}</p>
	    <el-checkbox v-model="agreeKyBot" @click="agreeKyBot = !agreeKyBot"></el-checkbox>
      <el-button type="text" style="font-size: 12px; margin-left: -8px;" @click="openAgreement">{{$t('hasAgree')}}</el-button>
	  </p>
	  <el-button @click="startService" :loading="startLoading" type="primary" :disabled="!agreeKyBot" class="btn-agree">{{$t('agreeAndOpen')}}</el-button>
	</div>
</template>
<script>
  import { mapActions } from 'vuex'
  import { handleSuccess, handleError } from '../../util/business'

  export default {
    name: 'help',
    props: ['propAgreement'],
    data () {
      return {
        agreeKyBot: false,
        startLoading: false
      }
    },
    methods: {
      openAgreement () {
        const h = this.$createElement
        this.$msgbox({
          title: this.$t('kybotAgreement'),
          message: h('p', {style: 'height: 500px; overflow: scroll'}, [
            h('pre', {}, this.$t('agreement'))
          ]),
          showCancelButton: false
        }).then(action => {

        })
      },
      ...mapActions({
        startKybot: 'START_KYBOT',
        getAgreement: 'GET_AGREEMENT',
        setAgreement: 'SET_AGREEMENT'
      }),
      // 同意协议并开启自动服务
      startService () {
        this.startLoading = true
        // 开启自动服务
        this.startKybot().then((resp) => {
          handleSuccess(resp, (data, code, status, msg) => {
            if (data) {
              this.$message({
                type: 'success',
                message: this.$t('openSuccess')
              })
              this.startLoading = false
              this.$emit('closeStartLayer')
              this.$emit('openSwitch')
            }
          })
        }).catch((res) => {
          handleError(res)
        })
        // 同意协议
        this.setAgreement().then((resp) => {
          handleSuccess(resp, (data, code, status, msg) => {
          })
        }, (res) => {
        })
      }
    },
    watch: {
      propAgreement: function (val) {
        if (!val) {
          this.agreeKyBot = false
        }
      }
    },
    locales: {
      'en': {agreeAndOpen: 'Enable Auto Upload', kybotAgreement: 'Kybot User Agreement', hasAgree: 'I have read and agree《KyBot Term of Service》', protocol: 'By analyzing your diagnostic package, KyBot can provide online diagnostic, tuning and support service for KAP. After starting auto upload service, it will automatically upload packages everyday regularly.', openSuccess: 'open successfully', agreement: `Terms and Conditions of Kyligence End User License
Version 2016-06-29
END USER LICENSE TERMS AND CONDITIONS
THESE TERMS AND CONDITIONS (THESE “TERMS”) APPLY TO
YOUR USE OF THE PRODUCTS (AS DEFINED BELOW) PROVIDED
BY SHANGHAI KYLIGENCE INFORMATION TECHNOLOGY, INC.
(“KYLIGENCE”).
PLEASE READ THESE TERMS CAREFULLY.
IF YOU (“YOU” OR “CUSTOMER”) PLAN TO USE ANY OF THE
PRODUCTS ON BEHALF OF A COMPANY OR OTHER ENTITY,
YOU REPRESENT THAT YOU ARE THE EMPLOYEE OR AGENT
OF SUCH COMPANY (OR OTHER ENTITY) AND YOU HAVE THE
AUTHORITY TO ACCEPT ALL OF THE TERMS AND CONDITIONS
SET AND THESE TERMS (COLLECTIVELY, THE “AGREEMENT”)
ON BEHALF OF SUCH COMPANY (OR OTHER ENTITY).
BY USING ANY OF THE PRODUCTS, YOU ACKNOWLEDGE AND
AGREE THAT:
(A) YOU HAVE READ ALL OF THE TERMS AND CONDITIONS OF
THIS AGREEMENT;
(B) YOU UNDERSTAND ALL OF THE TERMS AND CONDITIONS
OF THIS AGREEMENT;
(C) YOU AGREE TO BE LEGALLY BOUND BY ALL OF THE
TERMS AND CONDITIONS SET FORTH IN THIS AGREEMENT.
IF YOU DO NOT AGREE WITH ANY OF THE TERMS OR
CONDITIONS OF THESE TERMS, YOU MAY NOT USE ANY
PORTION OF THE PRODUCTS.
THE “EFFECTIVE DATE” OF THIS AGREEMENT IS THE DATE
YOU FIRST DOWNLOAD ANY OF THE PRODUCTS.
1. Product. For the purpose of this Agreement, “Product” shall mean any
of Kyligence’s products and software including but not limited to:
Kyligence Analytics Platform, any trial software, and any software, any
upgrades, updates, bug fixes or modified versions (collectively,
“Upgrades”) related to the foregoing. NOTWITHSTANDING ANY
OTHER PROVISION OF THE AGREEMENT: (1) CUSTOMER HAS
NO LICENSE OR RIGHT TO MAKE OR USE ANY ADDITIONAL
COPIES OR UPGRADES UNLESS CUSTOMER, AT THE TIME OF
MAKING OR ACQUIRING SUCH COPY OR UPGRADE, ALREADY
HOLDS A VALID LICENSE TO THE ORIGINAL SOFTWARE AND
HAS PAID THE APPLICABLE FEE TO AN APPROVED SOURCE
FOR THE UPGRADE OR ADDITIONAL COPIES; (2) USE OF
UPGRADES IS SUPPLIED BY AN APPROVED SOURCE FOR
WHICH CUSTOMER IS THE ORIGINAL END USER PURCHASER
OR OTHERWISE HOLDS A VALID LICENSE TO USE THE
PRODUCTS WHICH IS BEING UPGRADED; AND (3) THE MAKING
AND USE OF ADDITIONAL COPIES IS LIMITED TO NECESSARY
BACKUP PURPOSES ONLY.
2. Entire Agreement. This Agreement includes these Terms, any exhibits
or web links attached to or referenced in these Terms and any terms set
forth on the Kyligence web site at https://kybot.io This Agreement is the
entire agreement of the parties regarding the subject matter hereof,
superseding all other agreements between them, whether oral or written,
regarding the subject matter hereof.
3. License Delivery. Kyligence grants to Customer a nonexclusive,
nontransferable, nonsublicensable, revocable and limited license to access
and use the applicable Product as defined above in Section 1 solely for
Customer’s internal purposes. The Product is delivered via electronic
download or other approved source made available following Customer’s
acceptance of this Agreement.
4. License Restrictions. Unless expressly otherwise set forth in this
Agreement, Customer will not:
(a) modify, translate or create derivative works of the Product;
(b) decompile, reverse engineer or reverse assemble any portion of the
Product or attempt to discover any source code or underlying ideas or
algorithms of the Product;
(c) sell, assign, sublicense, rent, lease, loan, provide, distribute or
otherwise transfer all or any portion of the Product;
(d) make, have made, reproduce or copy the Product;
(e) remove or alter any trademark, logo, copyright or other proprietary
notices associated with the Product;
(f)use or permit the Products to be used to perform services for third
parties, whether on a service bureau or time sharing basis or otherwise,
without the express written authorization of Kyligence; or
(g) cause or permit any other party to do any of the foregoing.
5. Ownership. As between Kyligence and Customer and subject to the
grants under this Agreement, Kyligence owns all right, title and interest in
and to: (a) the Product (including, but not limited to, any upgrades,
modifications thereto or derivative works thereof); (b) all ideas, inventions,
discoveries, improvements, information, creative works and any other
works discovered, prepared or developed by Kyligence in the course of or
resulting from the provision of any services under this Agreement; and (c)
any and all Intellectual Property Rights embodied in the foregoing. For the
purpose of this Agreement, “Intellectual Property Rights” means any and
all patents, copyrights, moral rights, trademarks, trade secrets and any
other form of intellectual property rights recognized in any jurisdiction,
including applications and registrations for any of the foregoing. As
between the parties and subject to the terms and conditions of this
Agreement, Customer owns all right, title and interest in and to the data
generated by the use of the Products by Customer. There are no implied
licenses in this Agreement, and Kyligence reserves all rights not expressly
granted under this Agreement. No licenses are granted by Kyligence to
Customer under this Agreement, whether by implication, estoppels or
otherwise, except as expressly set forth in this Agreement.
6. Nondisclosure. “Confidential Information” means all information
disclosed (whether in oral, written, or other tangible or intangible form) by
Kyligence to Customer concerning or related to this Agreement or
Kyligence (whether before, on or after the Effective Date) which
Customer knows or should know, given the facts and circumstances
surrounding the disclosure of the information by Customer, is confidential
information of Kyligence. Confidential Information includes, but is not
limited to, the components of the business plans, the Products, inventions,
design plans, financial plans, computer programs, know-how, customer
information, strategies and other similar information. Customer will,
during the term of this Agreement, and thereafter maintain in confidence
the Confidential Information and will not use such Confidential
Information except as expressly permitted herein. Customer will use the
same degree of care in protecting the Confidential Information as
Customer uses to protect its own confidential information from
unauthorized use or disclosure, but in no event less than reasonable care.
Confidential Information will be used by Customer solely for the purpose
of using the Products in accordance with this Agreement. In addition,
Customer: (a) will not reproduce Confidential Information, in any form,
except for the purpose of using the Products in accordance with this
Agreement; and (b) will only disclose Confidential Information to its
employees and consultants who have a need to know such Confidential
Information in order to guarantee customer’s use of the Products in
accordance with this Agreement and if such employees and consultants
have executed a non-disclosure agreement with Customer with terms no
less restrictive than the non-disclosure obligations contained in this
Section. Confidential Information will not include information that: (i) is
in or enters the public domain without breach of this Agreement through
no fault of Customer; (ii) Customer can reasonably demonstrate was in its
possession prior to first receiving it from Kyligence; (iii) Customer can
demonstrate was developed by Customer independently and without use of
or reference to the Confidential Information; or (iv) Customer receives
from a third-party without restriction on disclosure and without breach of a
nondisclosure obligation. Notwithstanding any terms to the contrary in this
Agreement, any suggestions, comments or other feedback provided by 
Customer to Kyligence with respect to the Products (collectively,
“Feedback”) will constitute Confidential Information. Further, Kyligence
will be free to use, disclose, reproduce, license and otherwise distribute,
and exploit the Feedback provided to it as it sees fit, entirely without
obligation or restriction of any kind on account of Intellectual Property
Rights or otherwise. Subject to applicable law, in connection with the
performance of this Agreement and Customer’s use of the Kyligence
Products, (i) Kyligence agrees that it will not require Customer to deliver
to Kyligence any personally identifiable information (“PII”) and (ii)
Customer agrees not to deliver any PII to Kyligence.
7. Warranty Disclaimer. Customer represents warrants and covenants that:
(a) all of its employees and consultants will abide by the terms of this
Agreement; (b) it will comply with all applicable laws, regulations, rules,
orders and other requirements, now or hereafter in effect, of any applicable
governmental authority, in its performance of this Agreement.
Notwithstanding any terms to the contrary in this Agreement, Customer
will remain responsible for acts or omissions of all employees or
consultants of Customer to the same extent as if such acts or omissions
were undertaken by Customer. THE PRODUCTS ARE PROVIDED ON
AN “AS IS” OR “AS AVAILABLE” BASIS WITHOUT ANY
REPRESENTATIONS, WARRANTIES, COVENANTS OR
CONDITIONS OF ANY KIND. KYLIGENCE AND ITS SUPPLIERS
DO NOT WARRANT THAT ANY OF THE PRODUCTS WILL BE
FREE FROM ALL BUGS, ERRORS, OR OMISSIONS. KYLIGENCE
AND ITS SUPPLIERS DISCLAIM ANY AND ALL OTHER
WARRANTIES AND REPRESENTATIONS (EXPRESS OR IMPLIED,
ORAL OR WRITTEN) WITH RESPECT TO THE PRODUCTS
WHETHER ALLEGED TO ARISE BY OPERATION OF LAW, BY
REASON OF CUSTOM OR USAGE IN THE TRADE, BY COURSE OF
DEALING OR OTHERWISE, INCLUDING ANY AND ALL (I)
WARRANTIES OF MERCHANTABILITY, (II) WARRANTIES OF
FITNESS OR SUITABILITY FOR ANY PURPOSE (WHETHER OR
NOT KYLIGENCE KNOWS, HAS REASON TO KNOW, HAS BEEN
ADVISED, OR IS OTHERWISE AWARE OF ANY SUCH PURPOSE),
AND (III) WARRANTIES OF NONINFRINGEMENT OR CONDITION 
OF TITLE. CUSTOMER ACKNOWLEDGES AND AGREES THAT
CUSTOMER HAS RELIED ON NO WARRANTIES. IN ADDITION,
DUE TO THE CONTINUAL DEVELOPMENT OF NEW
TECHNIQUES FOR INTRUDING UPON AND ATTACKING
NETWORKS, KYLIGENCE DOES NOT WARRANT THAT THE
PRODUCTS OR ANY SYSTEM OR NETWORK ON WHICH THE
PRODUCTS IS USED WILL BE FREE OF VULNERABILITY TO
INTRUSION OR ATTACK.
8. Indemnification. Customer will indemnify, defend and hold Kyligence
and its directors, officers, employees, suppliers, consultants, contractors,
and agents (“Kyligence Indemnitees”) harmless from and against any and
all actual or threatened suits, actions, proceedings (at law or in equity),
claims (groundless or otherwise), damages, payments, deficiencies, fines,
judgments, settlements, liabilities, losses, costs and expenses (including,
but not limited to, reasonable attorney fees, costs, penalties, interest and
disbursements) resulting from any claim (including third-party claims),
suit, action, or proceeding against any Kyligence Indemnitees, whether
successful or not, caused by, arising out of, resulting from, attributable to
or in any way incidental to:
(a) any breach of this Agreement (including, but not limited to, any breach
of any of Customer’s representations, warranties or covenants);
(b) the negligence or willful misconduct of Customer; or
(c) the data and information used in connection with or generated by the
use of the Products.
9. Limitation of Liability. EXCEPT FOR ANY ACTS OF FRAUD, OR
WILLFUL MISCONDUCT, IN NO EVENT WILL: (A) KYLIGENCE
BE LIABLE TO CUSTOMER OR ANY THIRD-PARTY FOR ANY
LOSS OF PROFITS, LOSS OF DATA, LOSS OF USE, LOSS OF
REVENUE, LOSS OF GOODWILL, ANY INTERRUPTION OF
BUSINESS, ANY OTHER COMMERCIAL DAMAGES OR LOSSES,
OR FOR ANY INDIRECT, SPECIAL, INCIDENTAL, EXEMPLARY,
PUNITIVE OR CONSEQUENTIAL DAMAGES OF ANY KIND
ARISING OUT OF OR IN CONNECTION WITH THIS AGREEMENT
OR THE PRODUCTS (INCLUDING RELATED TO YOUR USE OR 
INABILITY TO USE THE PRODUCTS), REGARDLESS OF THE
FORM OF ACTION, WHETHER IN CONTRACT, TORT, STRICT
LIABILITY OR OTHERWISE, EVEN IF KYLIGENCE HAS BEEN
ADVISED OR IS OTHERWISE AWARE OF THE POSSIBILITY OF
SUCH DAMAGES; AND (B) KYLIGENCE’s TOTAL LIABILITY
ARISING OUT OF OR RELATED TO THIS AGREEMENT EXCEED
THE GREATER OF THE AGGREGATE OF THE AMOUNTS PAID OR
PAYABLE TO KYLIGENCE, IF ANY, UNDER THIS. MULTIPLE
CLAIMS WILL NOT EXPAND THIS LIMITATION. THE
FOREGOING LIMITATIONS, EXCLUSIONS AND DISCLAIMERS
SHALL APPLY TO THE MAXIMUM EXTENT PERMITTED BY
APPLICABLE LAW, EVEN IF ANY REMEDY FAILS ITS
ESSENTIAL PURPOSE.
10. Third-Party Suppliers. The Product may include software or other code
distributed under license from third-party suppliers (“Third Party
Software”). Customer acknowledges that such third-party suppliers
disclaim and make no representation or warranty with respect to the
Products or any portion thereof and assume no liability for any claim that
may arise with respect to the Products or Customer’s use or inability to use
the same.
11. Diagnostics and Reporting. Customer acknowledges that the product
contains a diagnostic functionality as its default configuration. The
diagnostic function collects configuration files, node count, software
versions, log files and other information regarding your environment and
use of the Products, and reports that information to Kyligence for use to
proactively identify potential support issues, to understand Customer’s
environment, and to enhance the usability of the Products. While
Customer may elect to change the diagnostic function in order to disable
regular automatic reporting or to report only on filing of a support ticket,
CUSTOMER AGREES THAT, NO LESS THAN ONCE PER
QUARTER, IT WILL RUN THE DIAGNOSTIC FUNCTION AND
REPORT THE RESULTS TO KYLIGENCE.
12. Termination. The term of this Agreement commences on the Effective
Date and continues for the period stated on Kyligence’s website, unless 
terminated for Customer’s breach of any material term herein.
Notwithstanding any terms to the contrary in this Agreement, in the event
of a breach of Sections 3, 4 or 6, Kyligence is entitled to immediately
terminate this Agreement. Upon expiration or termination of this
Agreement: (a) all rights granted to Customer under this Agreement will
immediately cease. In such event, Customer shall forthwith stop using the
Products; and (b) Customer will promptly provide Kyligence with all
Confidential Information (including, but not limited to the Products) then
in its possession or destroy all copies of such Confidential Information, at
Kyligence’s sole discretion and direction. Notwithstanding any terms to
the contrary in this Agreement, this sentence and the following Sections
will survive any termination or expiration of this Agreement: 4, 6, 7, 8, 9,
10, 12, 14, and 15.
13. Beta Software. In the event that Customer uses the functionality in the
Product for the purposes of downloading and installing any Kyligenceprovided
public beta software, such beta software will be subject either to
the Apache 2 license, or to the terms and conditions of the Public Beta
License as applicable.
14. Third Party Resources. Kyligence Products may include hyperlinks to
other web sites or content or resources (“Third Party Resources”), and the
functionality of such Kyligence Products may depend upon the availability
of such Third Party Resources. Kyligence has no control over any Third
Party Resources. You acknowledge and agree that Kyligence is not
responsible for the availability of any such Third Party Resources, and
does not endorse any advertising, products or other materials on or
available from such Third Party Resources. You acknowledge and agree
that Kyligence is not liable for any loss or damage which may be incurred
by you as a result of the availability of Third Party Resources, or as a
result of any reliance placed by you on the completeness, accuracy or
existence of any advertising, products or other materials on, or available
from, such Third Party Resources.
15. Miscellaneous. This Agreement will be governed by and construed in
accordance with the laws of the People’s Republic of China applicable to
agreements made and to be entirely performed within the People’s
Republic of China, without resort to its conflict of law provisions. The
parties agree that any action at law or in equity arising out of or relating to
this Agreement will be filed only in the courts located in Pudong New
District, Shanghai, China, and the parties hereby irrevocably and
unconditionally consent and submit to the exclusive jurisdiction of such
courts over any suit, action or proceeding arising out of this Agreement.
Upon such determination that any provision is invalid, illegal, or incapable
of being enforced, Kyligence remains the right to change and explain such
provisions. Any notices to be sent to Customer posted on the web site of
Kyligence shall be deemed having been sufficiently and legally delivered
to Customer. Except for payments due under this Agreement, neither party
will be responsible for any failure to perform or delay attributable in whole
or in part to any cause beyond its reasonable control, including but not
limited to fire, storm, floods, earthquakes, civil disturbances, disruption of
telecommunications, disruption of power or other essential services,
interruption or termination of service by any service providers being used
by Kyligence to link its servers to the Internet, labor disturbances,
vandalism, cable cut, computer viruses or other similar occurrences, or any
malicious or unlawful acts of any third-party. In the event of any such
delay the date of delivery will be deferred for a period equal to the time
lost by reason of the delay. Unless otherwise provided, any notice or
communication required or permitted to be given hereunder must be in
writing signed or authorized by the party giving notice, and may be
delivered by hand, deposited with an overnight courier, sent by confirmed
email, confirmed facsimile or mailed by registered or certified mail, return
receipt requested, postage prepaid, in each case to the address below or at
such other address as may hereafter be furnished in accordance with this
Section. No modification, addition or deletion, or waiver of any rights
under this Agreement will be binding on a party unless made in an
agreement clearly understood by the parties to be a modification or waiver
and signed by a duly authorized representative of each party. No failure or
delay (in whole or in part) on the part of a party to exercise any right or
remedy hereunder will operate as a waiver thereof or affect any other right
or remedy. All rights and remedies hereunder are cumulative and are not
exclusive of any other rights or remedies provided hereunder or by law. 
The waiver of one breach or default or any delay in exercising any rights
will not constitute a waiver of any subsequent breach or default.`},
      'zh-cn': {agreeAndOpen: '开启自动上传', kybotAgreement: 'Kybot用户协议', hasAgree: '我已阅读并同意《KyBot用户协议》', protocol: '通过分析生产的诊断包，提供KAP在线诊断、优化及服务，启动自动上传服务后，每天定时自动上传，无需自行打包和上传。', openSuccess: '开启成功', agreement: `跬智终端用户授权许可条款及条件
2016-06-29版本
终端用户授权许可条款及条件
本条款和条件(下称“条款”)适用于所有使用由上海跬智信息技术有限
公司(下称“跬智”)所提供产品(参见如下定义) 的用户。

请仔细阅读如下条款：

若您（下称“您”或“用户”）代表某公司或者其他机构使用任何产品
时，您特此陈述您作为该公司或该等其他机构的员工或代理，您有权代
表该公司或该等其他机构接受条款项下所要求的全部条款和条件（以下
统称为“本协议项”）。

若使用任何产品,您知晓并同意:
(A)您已阅读本协议中所有的条款和条件;
(B)您已理解本协议中所有的条款和条件;
(C)您已同意本协议中所有条款和条件对您具有法律约束力。
如果您不同意本协议所述条款和条件中的任意内容，则可以选择不使用
产品的任何部分。 
本协议的“生效日期”是指您第一次下载任何产品的日期。

1. 产品，指本协议项下任何跬智的产品和软件，包括但不限于：跬智
分析平台、任何试用软件、任何软件或与其相关的升级、更新、故障修
复或修改版本（统称“更新软件”）。无论本协议是否另有规定，（1）
用户不得自行复制或使用任何额外更新软件的副本，除非用户在使用或
获取该更新软件时，已拥有对原始软件有效的许可并已支付更新软件或
软件副本的相关费用；（2）更新软件的使用系经许可的资源，即作为
该等资源提供方的用户为最初的终端购买者或持有有效许可使用该正
在被更新产品的其他用户；（3）制作和使用额外的副本仅限于必要的
备份目的。
2. 全 部 协 议 , 本协议 包 括 本 条 款 以 及 任 何 跬 智 官 方 网 站
https://kybot.io 展示或者网页链接所附或引用的全部条款。本协议是
双方就相关事项达成的完整协议，取代跬智与用户之间就本条款相关事
项所达成的其他任何协议，无论是口头的还是书面的。
3. 使用许可，跬智授予用户非排他性的、不可转让的、非可再授权的，
可撤回的和有限的许可进行访问和使用第 1 条所定义的产品，该访问和
使用许可仅限于用户内部使用之目的。通过电子下载或其他经许可的来
源获得产品的用户均应受限于本协议的内容。
4. 许可限制，除非本协议另有明文规定，否则用户将不被允许：
（a）修改、翻译或制造产品的衍生作品；
（b）反向编译、反向工程、破解产品的任何部分或试图发现有关产品
的任何源代码、基本理念或运算方法；
（c）销售、分派、再授权、出租、出借、出质、提供或另行翻译全部
或部分产品；
（d）制造、获取非法制造的、再版或复制产品；
（e）删除或更改与产品相关联的任何商标、标志、版权或其他专有标
志；
（f）不得在没有跬智明确书面授权的情况下，使用或许可他人使用产品
为第三方提供服务，无论是在产品服务过程中使用或采用分时的方式；
（g）引起或许可其他任何其他方进行上述任何一种禁止行为。
5. 所有权，跬智和用户在本协议项下的许可需明确，跬智拥有以下各
项的全部权利、所有权和相关利益：（a）产品（包括但不限于，任何
更新软件、修订版本或其衍生作品）；（b）在跬智根据本协议提供任
何服务的过程中或作为其提供服务的结果，由跬智发现、产生或发展出
来的所有的概念、发明、发现、改进、信息、创意作品等；（c）前述
各项所含的任何知识产权权利。在本协议项下，“知识产权”是指在任
何管辖区域经申请和注册获得认可和保护的全部专利、版权、道德权利、
商标、商业秘密和任何其他形式的权利。跬智与用户同意，在受限于本
协议全部条款和条件的前提下，用户拥有使用产品而产生的数据的权
利、所有权等相关利益。在此协议中无任何默示许可，跬智保留本协议
项下未明确授权的全部权利。除非本协议明确约定，跬智在本协议下未
授予用户任何许可权利，无论是通过暗示、默许或其他方式。
6. 保密，保密信息是指，无论是在本协议生效前或生效后，由跬智披
露给用户的与本协议或与跬智相关的所有信息（无论是以口头的、书面、
或其他有形、无形的形式），而鉴于该等信息披露给客户时相关事实和
实际情况，客户知道或应当知道该等信息为跬智的保密信息。保密信息
包括但不限于，商业计划的内容、产品、发明、设计图纸、财务计划、
计算机程序、发明、用户信息、战略和其他类似信息。在本协议期限内，
除非获得明确许可，用户需保证保密信息的秘密性，并确保不会使用上
述保密信息。用户将采用与保护其自身保密信息的同等谨慎程度（不论
在何种情况下均不低于合理的谨慎程度）来保护跬智的保密信息，来避
免使得保密信息被未经授权的使用和披露。保密信息只供用户根据本协
议规定使用产品之目的而使用。此外，用户将：（a）除非用户为了根
据本协议的规定而使用产品之目的外，不得以任何形式复制保密信息；
（b）只向为确保用户可根据本协议使用产品而必需知道该保密信息的
员工和顾问披露保密信息，前提是上述员工和顾问已签署了包含保密义
务不低于本条所述内容的保密协议。保密信息不包括下列信息：（i）非
因用户过错违反本协议导致已进入公共领域的；（ii）用户能合理证明
其在通过跬智获得之前已知晓的；（iii）用户能证明没有使用或参考该
保密信息而独立获得的；（iv）用户从其他无披露限制或无保密义务
的第三方获得的。如无另行说明，由用户提供给跬智有关产品的任何
建议，评论或者其他反馈（统称“反馈信息”）将构同样成保密信息。
此外，跬智有权使用、披露，复制，许可和利用上述反馈信息，而无需
承担任何知识产权负担或其他任何形式的义务或限制的。根据相关法
律，与本协议的履行和用户使用跬智产品相关的情况下（i）跬智同意
不会要求用户提供任何个人身份信息；（ii）用户同意不提供任何个
人身份信息给跬智。
7. 免责声明，用户陈述、保证及承诺如下：（a）其所有员工和顾
问都将遵守本协议的全部条款；（2）在履行本协议时将遵守全部可
适用的政府部门颁发的法律、法规、规章、命令和其他要求（无论
是现行有效还是之后生效的）。无论本协议是否另有规定，用户将
持续对其雇员或顾问的全部作为或不作为承担责任，如同该等作为
或不作为系其自身所为。产品系按照原状或现状提供给用户，不含
任何形式的陈述、保证、承诺或条件。跬智及其供应商不保证任何
产品将无任何故障、错误或漏洞。跬智和其供应商不为产品的如下
内容提供任何陈述和保证（无论是明示或暗示，口头或书面），不
论该内容是否依据法律之规定，行业惯例，交易习惯或其他原因而
要求的：(I)保证适销性；(II)保证可适用于任何目的（不论跬智是否
知晓、应当知晓、被建议或另行得知该目的）；(III)保证不侵权和拥
有全部所有权。用户已明确知悉并同意产品上无任何陈述和保证。
此外，鉴于进行入侵和网络攻击的新技术在不断发展，跬智并不保
证产品或产品所使用的系统或网络将免于任何入侵或攻击。
8. 损害赔偿，用户应赔偿、保护或使得跬智及其董事、高管、雇
员、供应商、顾问、承包商和代理商（统称为“跬智受保障方”）
免受所有现存或潜在的针对跬智受保障方因提起请求、诉讼或其他
程序而引起的要求其赔偿损害损失、支付费用、罚款、调解、损失
费用等支出（包括但不限于合理的律师费、费用、罚款、利息和垫
付款），用户承担上述责任的前提是该请求、诉讼或其他程序，不
论是否成功系在如下情况发生时导致、引起的，或以任何形式与下
述情况相关的：
（a）任何对本协议的违反（包括但不限于，任何违反用户陈述和
保证或约定的情况）；
（b）用户过失或故意产生的过错行为；或
（c）引起争议的数据和信息系在产品的使用过程中产生或收集的。
9. 责任限制，除了跬智存在欺诈或故意的过错行为，在任何情况
下：（A）跬智都不会赔偿用户或任何第三方的因本协议或产品（包
括用户使用或无法使用产品的情况）而遭受的任何利润损失、数据
损失、使用损失、收入损失、商誉损失、任何经营活动的中断，任
何其他商业损害或损失，或任何间接的、特殊的、附带的、惩戒性、
惩罚性或伴随的损失，不论上述损失系因合同、侵权、严格责任或
其他原因而确认的，即使跬智已被通知或因其他可能的渠道知晓上
述损失发生的可能性；（B）跬智因本协议所需承担的全部赔偿责任
不应超过已支付或将支付给跬智的全部款项总额（若有）。多项请
求亦不得超过该金额限制。上述限制、排除情况及声明应在相关法
律允许的最大范围内得以适用，即便任何补偿无法达到其实质目
的。
10. 第三方供应商，产品可能包括由第三方供应商许可提供的软件
或其他代码（下称“第三方软件”）。用户已知悉第三方供应商不
对产品或其任何部分提供任何陈述和保证，不承担因产品或用户对
第三方软件的使用或不能使用的情况而产生的任何责任。
11. 诊断和报告，用户了解并同意该产品包含诊断功能作为其默认
配置。诊断功能用于收集有关使用环境和产品使用过程中的配置文
件、节点数、软件版本、日志文档和其他信息，并将上述信息报告
给跬智用于提前识别潜在的支持问题、了解用户的使用环境、并提
高产品的使用性能。虽然用户可以选择更改诊断功能来禁用自动定
时报告或仅用于报告服务记录，但用户需同意，每季度须至少运行
一次诊断功能并将结果报告给跬智。
12. 终止，本协议期限从生效之日起直到跬智网站规定的期限终止，
除非用户违反本协议中条款而提前终止。无论本协议是否另有规
定，在用户存在违反第 3、4 或 6 条时，跬智有权立即终止本协议。
本协议期满或提前终止时：（a）根据本协议所授予给用户的所有权
利将立即终止，在此情况下用户应立即停止使用产品；（b）用户将
及时将届时仍由其占有的所有保密信息（包括但不限于产品）提供
给跬智，或根据跬智的自行审慎决定及指示，销毁该等保密信息全
部副本。无论本协议是否另有规定，本条及以下条款在本协议到期
或终止时仍有效：第 4、6、7、8、9、10、12、14 和 15 条。
13. 测试版软件，在用户使用产品用作下载和安装任何由跬智提供
的公共测试版软件，该测试版软件应适用 Apache 2 许可或其他对公
共测试版许可适用的条款和条件。
14. 第三方资源， 跬智供应的产品可能包括对其他网站、内容或资
源的超链接（下称“第三方资源”），且跬智此类产品的正常使用
可能依赖于第三方资源的可用性。跬智无法控制任何第三方资源。
用户承认并同意，跬智不就第三方资源的可用性承担任何责任，也
不对该等第三方资源所涉及的或从其中获得的任何广告、产品或其
他材料提供保证。用户承认并同意，跬智不应因第三方资源的可用
性、或用户依赖于第三方资源所涉及的或从其中获得的任何广告、
产品或其他材料的完整性、准确性及存续而可能遭受的损失或损害
承担任何责任。
15. 其他，本协议全部内容均在中华人民共和国境内履行，受中华
人民共和国法律管辖并根据其解释（但不适用相关冲突法的法律条
款）。用户与跬智同意与本协议有关的任何争议将向上海市浦东新
区法院提出，且不可撤销无条件的同意上述有法院对因本协议提起
的全部诉讼、争议拥有排他的管辖权。一旦确定任何条款无效，非
法或无法执行，跬智保留修改和解释该条款的权利。任何需要发送
给用户的通知如公布在跬智的网站上则被视为已有效、合法地发送
给用户。除了本合同项下应支付款项的义务外，任何一方将不对因
不可抗力而导致的无法合理控制的全部或部分未能履行或延迟履
行本协议的行为负责，不可抗力包括但不限于火灾、暴风雨、洪水、
地震、内乱、电信中断、电力中断或其他基础设施的中断、跬智使
用的服务提供商存在问题导致服务中断或终止、罢工、故意毁
坏事件、电缆被切断、病毒入侵或其他任意第三方故意或非法
的行为引起的其他类似事件。在上述迟延履行情况出现时，可
延迟履行协议的时间为因上述原因引起的延迟时间。本协议另
有明确规定外，本协议所要求或认可的通知或通讯均需以书面
形式经一方有权代表签署或授权并以直接呈递、隔夜快递，经
确认的电子邮件发送，经确认的传真或邮寄挂号信、挂号邮件
保留回单，预付快递费用的方式送达至以下地址或本条款所提
供的其他地址。对本协议的任何修改、补充或删除或权利放弃，
必须通过书面由双方适当授权的代表签署确认后方为有效。任
何一方对任何权利或救济的不履行或迟延履行（部分或全部）
不构成对该等权利或救济的放弃，也不影响任何其他权利或救
济。本协议项下的所有权利主张和救济均可为累积的且不排除本协
议中包含的或法律所规定的其他任何权利或救济。对本协议中任何
一项违约责任的豁免或延迟行使任何权利，并不构成对其他后续违
约责任的豁免。
`}
    }
  }

</script>
<style lang="less">
  @import url(../../less/config.less);
  .start_kybot {
    text-align: left;
    .btn-agree {
      display: block;
      margin: 20px auto;
    }
    .blue {
      color: #20a0ff;
    }
    .el-checkbox{
      color: @content-color;
    }
    pre {
      white-space: pre-wrap; /* css-3 */
      white-space: -moz-pre-wrap; /* Mozilla, since 1999 */
      white-space: -pre-wrap; /* Opera 4-6 */
      white-space: -o-pre-wrap; /* Opera 7 */
      word-wrap: break-word; /* Internet Explorer 5.5+ */
    }
  }
</style>
