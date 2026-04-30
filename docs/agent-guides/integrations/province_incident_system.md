# Provincial Incident System Interface Guide

This document summarizes the provincial police incident system captured from `http://68.29.179.170/dsjfxxb`. Use it when a task mentions the provincial incident system, `省厅警情系统`, `68.29.179.170`, or `/dsjfxxb`.

## Basics

- Base URL: `http://68.29.179.170/dsjfxxb`
- Related capture: `docs/agent-guides/integrations/68.29.179.170.har`
- System role: provincial incident/call data source. It is similar in purpose to the city `DSJJQFX` system documented in `docs/agent-guides/integrations/dsjjqfx.md`, but provincial data can differ from city data.
- Covered business endpoints:
  - `/cmd/selectFirstCmdList`
  - `/nature/treeViewData`
  - `/mark/treeViewData`
  - `/case/list`
  - dashboard statistics under `/daily`, `/weekly`, `/monthly`, and `/amount`
- Query requests rely on an authenticated browser/service session. Confirm the login/session behavior before adding direct integration code.
- The HAR contains sensitive business data and captured authentication material. Do not copy real caller phones, ID card numbers, operator IDs, detailed incident contents, cookies, or passwords into code, tests, logs, or documentation.

## Authentication

The captured browser flow logs in through:

- Method: `POST`
- URL: `http://68.29.179.170/dsjfxxb/login`
- Content type: `application/x-www-form-urlencoded`
- Form fields:
  - `username`
  - `password`
  - `rememberMe`

The browser then uses cookies such as `JSESSIONID` and `rememberMe`.

Usage notes:

- The captured password value is frontend-encoded or encrypted. Do not treat the HAR as a credential source.
- Integration code should use environment-provided credentials or a shared session manager pattern, not hardcoded values.
- Avoid import-time login or network traffic. Log in lazily when a request actually needs the upstream session.

## 1. `/cmd/selectFirstCmdList`

### Purpose

Returns the first-level provincial area or command list. Typical uses:

- Loading city-level area options
- Mapping area IDs to area display names
- Initial map/dashboard area selection

### Request

- Method: `GET`
- Observed URL in HAR: `http://68.29.179.170/dsjfxxb//cmd/selectFirstCmdList`

The HAR uses a double slash before `cmd`. Existing code should tolerate the captured form. If trying to simplify it to `/dsjfxxb/cmd/selectFirstCmdList`, verify against the live system first.

### Response shape

The response is an array. Common node fields:

- `id`: area code, for example a city-level Guangdong code
- `name`: short display name
- `fullName`: full display name
- `polyline`: map boundary data, potentially large
- `params`: extra metadata object

### Usage notes

- The captured response contains 22 first-level area entries.
- If only code/name mapping is needed, avoid retaining `polyline` in normalized business records.

## 2. `/nature/treeViewData`

### Purpose

Returns the incident nature tree used by the provincial system. Typical uses:

- Incident-nature selectors
- Mapping between nature code and display name
- Filtering `/case/list` by confirmed or original incident nature

### Request

- Method: `GET`
- URL: `http://68.29.179.170/dsjfxxb/nature/treeViewData`

The selector page is:

- Method: `GET`
- URL: `http://68.29.179.170/dsjfxxb/nature/selectNatureTreeCaseNode?isCheck=true&type=chara`

### Response shape

The response is an array of tree nodes. Common fields:

- `id`: nature code
- `name`: nature display name
- `pinYin`: pinyin
- `firstChar`: pinyin initials
- `title`: usually `name(code)`
- `checked`, `chkDisabled`, `open`, `nocheck`: tree widget state fields

### Usage notes

- The captured tree contains 1376 nodes.
- Store `id` as the real filter value and use `name` for display.
- Provincial nature codes and city `DSJJQFX` nature codes may not always align. Do not assume a display-name match means the same business category without checking.

## 3. `/mark/treeViewData`

### Purpose

Returns the incident tag tree. This endpoint is important for thematic filters such as minors-related incidents.

### Request

- Method: `GET`
- URL: `http://68.29.179.170/dsjfxxb/mark/treeViewData`

The selector page is:

- Method: `GET`
- URL: `http://68.29.179.170/dsjfxxb/mark/selectMarkTree?isCheck=true&markCode=`

### Response shape

The response is an array of tree nodes. Common fields:

- `id`: tag code
- `name`: tag display name
- `pinYin`: pinyin
- `firstChar`: pinyin initials
- `title`: usually `name(code)`
- `checked`, `chkDisabled`, `open`, `nocheck`: tree widget state fields

### Usage notes

- The captured tree contains 500 nodes.
- Minors-related filtering uses `caseMarkNo` and `caseMark` in `/case/list`.
- Captured minors tag examples include `未成年人`, `未成年人（加害方）`, `未成年人（受害方）`, and `未成年人（其他）`.

## 4. `/case/list`

### Purpose

Returns paginated incident detail rows. This is the primary endpoint for provincial incident detail retrieval and minors-related incident statistics.

Compared with the city `DSJJQFX` `/case/list`, this provincial endpoint uses `params[startTime]` and `params[endTime]` for the alarm-time range and uses provincial field names such as `charaNo`, `oriCharaNo`, and `caseMarkNo`.

### Request

- Method: `POST`
- URL: `http://68.29.179.170/dsjfxxb/case/list`
- Content type: `application/x-www-form-urlencoded`

### Core request parameters

- `params[startTime]`: alarm start time, format `YYYY-MM-DD HH:mm:ss`
- `params[endTime]`: alarm end time, format `YYYY-MM-DD HH:mm:ss`
- `pageSize`: page size
- `pageNum`: page number, starting from `1`
- `orderByColumn`: sort field, commonly `alarmTime`
- `isAsc`: sort direction, commonly `desc`

### Minors incident parameters

Use these fields when querying minors-related incident data:

- `params[startTime]`: alarm start time
- `params[endTime]`: alarm end time
- `charaNo`: confirmed incident nature code
- `chara`: confirmed incident nature name
- `oriCharaNo`: original incident nature code
- `oriChara`: original incident nature name
- `caseMarkNo`: incident tag code
- `caseMark`: incident tag name

Important: the captured request field is `charaNo`, not `charNo`.

Typical minors filter shape:

```text
params[startTime]=YYYY-MM-DD 00:00:00
params[endTime]=YYYY-MM-DD 23:59:59
charaNo=01,02
chara=刑事类警情,行政（治安）类警情
caseMarkNo=01020201,0102020101,0102020102,0102020103
caseMark=未成年人,未成年人（加害方）,未成年人（受害方）,未成年人（其他）
pageSize=100
pageNum=1
orderByColumn=alarmTime
isAsc=desc
```

### Other observed request parameters

These fields were present in the captured form and are often left empty or set to `全部` when not filtering:

- `caseSourceCode` / `caseSourceName`
- `caseNo`
- `dutyDeptNo` / `dutyDeptName`
- `callerPhone`
- `occurAddress`
- `callerPeopleName`
- `phoneAddress`
- `callerAddress`
- `iniCharaNo` / `iniChara`
- `fixCharaNo` / `fixChara`
- `caseLevel`
- `operatorName`
- `callerPeopleIdcard`
- `uploadAreaNo`
- `fixCaseSourceCode` / `fixCaseSourceName`
- `dossierNo`
- `firstOriCharaNo` / `firstOriChara`
- `firstCharaNo` / `firstChara`
- `handleResultNo`

### Common response structure

Top-level fields:

- `code`: result code
- `total`: total matching record count
- `rows`: current page of incident rows

Frequently observed fields inside `rows`:

- Incident identity: `caseNo`, `associationCaseNo`
- Times: `createTime`, `updateTime`, `callTime`, `alarmTime`, `occurTime`, `callEndTime`, `noticeTime`, `signTime`, `actSignDur`
- Source fields: `caseSourceCode`, `caseSourceName`, `fixCaseSourceCode`, `fixCaseSourceName`, `recvType`, `recvTypeName`, `callTypeNo`, `callTypeName`
- Nature fields: `charaNo`, `charaName`, `oriCharaNo`, `oriCharaName`, `iniCharaNo`, `iniCharaName`, `fixCharaNo`, `fixCharaName`, `firstCharaNo`, `firstCharaName`, `firstOriCharaNo`, `firstOriCharaName`
- Nature hierarchy: `charaFirstNo`, `charaFirstName`, `charaSecondNo`, `charaThirdNo`, `oriCharaFirstNo`, `oriCharaFirstName`, `oriCharaSecondNo`, `oriCharaThirdNo`
- Area and department: `uploadAreaNo`, `uploadAreaName`, `dutyDeptNo`, `dutyDeptName`, `areaNo`
- Content and address: `caseContents`, `firstCaseContents`, `supplementCaseContents`, `occurAddress`, `phoneAddress`, `callerPhoneAddress`
- Caller and operator fields: `callerPeopleName`, `callerPhone`, `callerPhoneName`, `callerPeopleGenderCode`, `callerPeopleGenderName`, `operatorNo`, `operatorName`, `operatorIdcard`
- Location and recording: `lngOfCall`, `latOfCall`, `satelliteLngOfCall`, `satelliteLatOfCall`, `lngOfLocate`, `latOfLocate`, `recordingId`, `recordingDuration`, `recordingText`
- Handling fields: `caseLevel`, `caseLevelName`, `caseHandleCode`, `caseHandleName`
- Miscellaneous: `params`, `index`, `internetTerminal`, `associationCharaNo`, `linkPhone`

### Usage notes

- Treat `/case/list` rows as sensitive incident records. Redact caller phone numbers, ID card numbers, exact addresses, operator identity, coordinates, recording IDs, and detailed incident text in logs and test fixtures.
- Use time-window plus pagination. Set a page cap for automation so a bad filter cannot trigger unbounded upstream requests.
- Normalize provincial raw fields in an adapter layer before feeding project business logic. Good internal names include `case_no`, `alarm_time`, `occur_time`, `nature_code`, `nature_name`, `tag_code`, `tag_name`, `dept_code`, and `dept_name`.
- When comparing provincial data with city `DSJJQFX` data, expect differences in coverage, correction state, category fields, and tag availability.

## 5. Dashboard statistics endpoints

The captured page also calls several dashboard endpoints. These are useful for visual summaries but are not a replacement for `/case/list` when per-incident detail is required.

### Period and category charts

Observed endpoints:

- `GET /monthly/caseStatisticsByPeriod`
- `POST /daily/caseStatisticsByPeriod`
- `POST /daily/caseStatisticsByNature`
- `POST /daily/caseStatisticsByCaseSource`
- `GET /weekly/criminalCaseStatistics`
- `GET /weekly/securityCaseStatistics`

Common response shape:

- `xAxis`: labels
- `series`: chart series array

### Area statistics

- Method: `GET`
- URL: `http://68.29.179.170/dsjfxxb/amount/caseStatisticsByArea`

Response shape:

- `list`: area statistic rows

Fields inside `list`:

- `name`: area name
- `value`: displayed statistic value
- `avgValue`: average or comparison value
- `actValue`: actual value

### Warning threshold cards

Observed endpoints:

- `POST /daily/caseStatisticsTotal`
- `POST /daily/caseStatisticsCriminal`
- `POST /daily/caseStatisticsExecutive`
- `POST /daily/caseStatisticsTraffic`
- `POST /daily/caseStatisticsFire`

Common response fields:

- `actualVal`: current value
- `maxGreenVal`
- `maxYellowVal`
- `maxOrangeVal`
- `maxRedVal`
- `rateOfRise`
- `params`

Usage notes:

- In the HAR, several dashboard `POST` requests were sent with no form body and appear to use page defaults or server-side current-period defaults.
- Verify the required time-window behavior with the live system before using these endpoints in backend reports.

## Endpoint Selection

When code touches `http://68.29.179.170` or `/dsjfxxb`, use this rule of thumb:

- Need provincial area metadata: use `/cmd/selectFirstCmdList`
- Need incident nature metadata: use `/nature/treeViewData`
- Need incident tag metadata: use `/mark/treeViewData`
- Need per-incident detail rows or minors-related incident statistics: use `/case/list`
- Need dashboard-only summary charts: use the `/daily`, `/weekly`, `/monthly`, or `/amount` endpoints

If a feature compares city and provincial incident data, load both integration guides first:

1. `docs/agent-guides/integrations/dsjjqfx.md`
2. `docs/agent-guides/integrations/province_incident_system.md`

