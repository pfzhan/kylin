
exports.default = {
  common: {
    // 常规操作
    add: 'Add',
    edit: 'Edit',
    delete: 'Delete',
    drop: 'Drop',
    cancel: 'Cancel',
    close: 'Close',
    update: 'Update',
    save: 'Save',
    ok: 'OK',
    submit: 'Submit',
    setting: 'Settings',
    logout: 'Log Out',
    sync: 'Sync',
    clone: 'Clone',
    check: 'Check',
    view: 'View',
    detail: 'Detail',
    draft: 'Draft',
    overview: 'Overview',
    zoomIn: 'Zoom In',
    zoomOut: 'Zoom Out',
    automaticlayout: 'Automatic Layout',
    // 常规状态
    success: 'Success',
    fail: 'Fail',
    running: 'Has a running job',
    status: 'Status',
    // 术语
    model: 'Model',
    project: 'Project',
    projects: 'Projects',
    cube: 'Cube',
    models: 'Models',
    jobs: 'Jobs',
    cubes: 'Cubes',
    dataSource: 'Datasource',
    fact: 'Fact Table',
    limitfact: 'Lookup Table(limited)',
    lookup: 'Lookup Table',
    computedColumn: 'Computed Column',
    pk: 'Primary key',
    fk: 'Foreign key',
    manual: 'Documentation',
    tutorial: 'Tutorial',
    qa: 'FAQ',
    // 通用提示
    unknownError: 'Unknown Error.',
    submitSuccess: 'Submitted successfully',
    addSuccess: 'Added successfully',
    saveSuccess: 'Saved successfully',
    cloneSuccess: 'Cloned successfully',
    delSuccess: 'Deleted successfully',
    backupSuccess: 'Back up successfully',
    updateSuccess: 'Updated successfully',
    actionSuccess: 'Operate successfully',
    confirmDel: 'Confirm delete it?',
    checkDraft: 'Detected the unsaved content, are you going to continue the last edit?',
    // placeholder
    pleaseInput: 'Please input here',
    pleaseFilter: 'filter...',
    enterAlias: 'enter alias',
    pleaseSelect: 'Please select',
    pleaseSelectUserName: 'Please select user name',
    pleaseSelectColumnName: 'Please select column name',
    pleaseSelectSampleRange: 'Please select sampling percentage',
    noData: 'No data',
    checkNoChange: 'No content changed',
    // 格式提示
    nameFormatValidTip: 'Invalid name! Only letters, numbers and underscore characters are supported in a valid name.',
    // 其他
    users: 'Users',
    tip: 'Notice',
    action: 'Action',
    help: 'Help',
    username: 'username',
    password: 'password',
    saveDraft: 'Responding to a request for saving drafts，Please try again later.',
    streamingConnectHiveError: 'Can not connect hive table with streaming table',
    seeDetail: 'Details >>'
  },
  model: {
    modelName: 'Model Name:',
    modelNameGrid: 'Model Name',
    modifiedGrid: 'Last Upated Time',
    statusGrid: 'Health Status',
    metaData: 'Meta Data',
    checkData: 'Sample Data',
    ownerGrid: 'Owner',
    modelDesc: 'Model Description',
    samplingSetting: 'Sampling Setting:',
    checkModel: 'Check Model',
    scanRangeSetting: 'Time range setting',
    sameModelName: 'Model with the same name existed',
    modelNameTips: '1.Model name is unique name of entire system.<br/> 2.Can not edit model name after created.',
    partitionDateTip: '1.Partition date column not required, leave as default if cube always need full build.<br/> 2.Column should contain date value (type can be Date, Timestamp, String, VARCHAR, Int, Integer, BigInt, etc.)',
    partitionSplitTip: 'For cases where fact table saves date and hour at two columns.',
    partitionTimeTip: 'Column should contain time value (type can be Timestamp, String, VARCHAR, etc.)',
    modelCheckTips1: 'Model health check is highly recommended to help generate a cube optimize result.',
    modelCheckTips2: 'Trigger model check will send a job (executing time depends ondataset scale and sampling setting), you can view it on monitor page.',
    samplingSettingTips: 'Here you can set time range and sampling ratio based on your demand and cluster resource.',
    samplingPercentage: 'Sampling percentage:',
    timeRange: 'Time range',
    samplingPercentageTips: 'If sampling ratio is high, check job would return accurate results <br/> with high resource engaged. If sampling ratio is low, check job would <br/>return less accurate results with resource saving.',
    modelHasJob: 'This model has a running job of model check, thus the action is disabled.',
    viewModeLimit: 'This action is disabled in view mode.',
    modelCheck: 'Model health check is highly recommended to help generate a cube optimize result.',
    dragTip: 'Try to drag the tables from the left',
    resetCheckDataTip: 'Please note that model check results would be inconsistent with the new editing. So model check results would be dropped after the new editing saved.',
    dimensionLinkLimit: 'Only dimension columns can be linked',
    computedLinkLimit: 'Computed column cannot be join key.'
  },
  cube: {
    inputSqlTip1: '1.The following shows all the inputing SQL statements, each with ";" as a split.',
    inputSqlTip2: '2.Incorrect inputing SQL statements will be ignored and will not work on subsequent steps.',
    readyCubeTip: 'Cube containing segments is not allowed to be modified, avoiding the inconsistence of cube metadata.',
    saveCubeTip: 'Confirm to save the cube?',
    cubeName: 'Cube Name',
    scheduler: 'Scheduler',
    merge: 'Merge Settings',
    maxGroupColumn: 'Max Dimension Combination:',
    schedulerTip: 'Via scheduler, users can set up an incremental build plan: <br/>after triggering the first time build, the new data will be built incrementally at a fixed period(build cycle).',
    cubeNameTip: '1.Cube name is unique name of entire system.<br/>2.Can not edit cube name after created.',
    noticeTip: 'These statuses will trigger an email notification.',
    dimensionTip: '1.In fact table: one normal dimension will be auto generated per column.<br/>2.In lookup table: you can choose to generate a normal dimension or a derived dimension.',
    expressionTip: 'All cubes have to contain one measure for Count(1), suggest use "_Count_" as name (Has been generated automatically)',
    paramValueTip: '1.Only accept single column in param value with "Column" type.<br/>2.Distinct Count is approximate, please indicate Error Rate, higher accuracy degree accompanied with larger storage size and longer build time.',
    orderSumTip: 'Will use this column for SUM and Order by. For constant value 1, will use SUM(1).',
    hostColumnTip: 'Host column is the dimension to derive from, e.g. page_id.',
    refreshSetTip: 'The thresholds will be checked ascendingly to see if any consectuive segments\' time range has exceeded it. <br/>For example the [ 7 days, 30 days] will result in daily incremental segments being merged every 7 days, and the 7-days segments will get merged every 30 days.',
    // for column encoding
    dicTip: 'Dict encoding applies to most columns and is recommended by default. But in the case of ultra-high cardinality, it may cause the problem of insufficient memory.',
    fixedLengthTip: 'Fixed-length encoding applies to the ultra-high cardinality scene, and will select the first N bytes of the column as the encoded value. When N is less than the length of the column, it will cause the column to be truncated; when N is large, the Rowkey is too long and the query performance is degraded.',
    intTip: 'Deprecated, please use integer encoding instead.',
    integerTip: 'For integer characters, the supported integer range is [-2 ^ (8 * N-1), 2 ^ (8 * N-1)].',
    fixedLengthHexTip: 'Use a fixed-length("length" parameter) byte array to encode the hex string dimension value, supporting formats include yyyyMMdd, yyyy-MM-dd, yyyy-MM-dd HH: mm: ss, yyyy-MM-dd HH: mm: ss.SSS (the section containing the timestamp in the column will be truncated).',
    dataTip: 'Use 3 bytes to encode date dimension value.',
    timeTip: 'Use 4 bytes to encode timestamp, supporting from 1970-01-01 00:00:00 to 2038/01/19 03:14:07. Millisecond is ignored. ',
    booleanTip: 'Use 1 byte to encode boolean value, valid value including: true, false; TRUE, FALSE; True, False; t, f; T, F; yes, no; YES, NO; Yes, No; y, n, Y, N, 1, 0.',
    orderedbytesTip: '',
    sameCubeName: 'Cube with the same name existed',
    inputCubeName: 'Please input a cube name',
    addCube: 'Add Cube',
    cubeHasJob: 'This cube has a running job of cube build, thus the action is disabled.',
    selectModelName: 'Please select a model',
    optimizerInputTip: 'The model check result is a necessary condition for the cube optimizer to work. Add SQL patterns can help the optimizer output <br/>suggestions that are capable to fill query demand. The cube optimizer could provide suggestions on two steps:  dimension and measure.',
    rowkeyTip: '<h4>What is Rowkeys?</h4><p>Rowkeys specifies how dimensions are organized. </p><h4>What is Shard By column? </h4><p>If specified as "true", cube data will be sharded according to its value. </p><h4>RowKey Encoding</h4><ol><li>"dict" Use dictionary to encode dimension values. dict encoding is very compact but vulnerable for ultra high cardinality dimensions. </li><li>"boolean" Use 1 byte to encode boolean values, valid value include: true, false, TRUE, FALSE, True, False, t, f, T, F, yes, no, YES, NO, Yes, No, y, n, Y, N, 1, 0</li><li>"integer" Use N bytes to encode integer values, where N equals the length parameter and ranges from 1 to 8. [ -2^(8*N-1), 2^(8*N-1)) is supported for integer encoding with length of N. </li><li>"int" Deprecated, use latest integer encoding instead. </li><li>"date" Use 3 bytes to encode date dimension values. </li><li>"time" Use 4 bytes to encode timestamps, supporting from 1970-01-01 00:00:00 to 2038/01/19 03:14:07. Millisecond is ignored. </li><li>"fix_length" Use a fixed-length("length" parameter) byte array to encode integer dimension values, with potential value truncations. </li><li>"fixed_length_hex" Use a fixed-length("length" parameter) byte array to encode the hex string dimension values, like 1A2BFF or FF00FF, with potential value truncations. Assign one length parameter for every two hex codes. </li></ol>'
  },
  project: {
    mustSelectProject: 'Please select a project first',
    selectProject: 'Please select a project',
    projectList: 'Project List',
    addProject: 'Add Project'
  },
  query: {
    saveQuery: 'Save Query',
    name: 'Name:',
    desc: 'Description:',
    querySql: 'Query SQL:',
    queryEngine: 'Query Engine: ',
    visualization: 'Visualization',
    grid: 'Grid',
    export: 'Export',
    status: 'Status:',
    startTime: 'Start Time:',
    duration: 'Duration:',
    project: 'Project:',
    graphType: 'Graph Type:',
    dimension: 'Dimensions:',
    metrics: 'Metrics:'
  },
  job: {
  },
  dataSource: {
    columnName: 'Column',
    cardinality: 'Cardinality',
    dataType: 'Data Type',
    comment: 'Comment',
    columns: 'Columns',
    samplingPercentage: 'Sampling percentage:',
    extendInfo: 'Extend Information',
    statistics: 'Statistics',
    sampleData: 'Sample Data',
    maximum: 'Max Value',
    minimal: 'Min Value',
    nullCount: 'Null Count',
    minLengthVal: 'Min Length Value',
    maxLengthVal: 'Max Length Value',
    expression: 'Expression',
    returnType: 'Data Type',
    tableName: 'Table Name:',
    lastModified: 'Last Modified:',
    totalRow: 'Total Rows:',
    collectStatice: 'The higher the sampling percentage, the more accurate the stats information, the more resources engaging.',
    dataSourceHasJob: 'This table has a running job of cube build, thus the action is disabled.'
  },
  login: {

  },
  menu: {
    dashboard: 'Dashboard',
    studio: 'Studio',
    insight: 'Insight',
    monitor: 'Monitor',
    system: 'System',
    project: 'Project'
  },
  user: {
    tip_password_unsafe: 'The password should contain at least one number, letter and special character（~!@#$%^&*(){}|:"<>?[];\',./`).'
  },
  system: {
    evaluationStatement: 'You are using KAP with Evaluation License. For more product information, expert consulting and services, please <a target="_blank" href="mailto:g-ent-lic@kyligence.io" style="font-weight:bold">contact us</a>. We’ll get you the help you need from Apache Kylin core team.',
    statement: 'You have purchased KAP with Enterprise License and services. If you encounter any problems in the course of use, please feel free to <a target="_blank" href="mailto:g-ent-lic@kyligence.io" style="font-weight:bold">contact us</a>. We will provide you with consistent quality products and services.'
  },
  kybotXY: {
    agreement: `
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
will not constitute a waiver of any subsequent breach or default.`
  }
}
