# LinkedIn Post Drafts - Module 4 (dbt)

## 1) dbt 프로젝트 총정리
🚀 Data Engineering Zoomcamp Module 4 완료!

이번 주에는 dbt로 NYC Taxi 데이터를 Analytics-ready 형태로 만들었습니다.

핵심적으로 배운 것:
- staging/intermediate/mart 레이어 분리 설계
- `ref()`로 모델 의존성 관리 + lineage 이해
- `dbt run`, `dbt test`, `dbt build`의 차이와 사용 시점
- generic test(`not_null`, `unique`, `accepted_values`)로 데이터 품질 검증
- 월/존/서비스 타입 기준 매출 데이터마트 구성

특히 인상적이었던 점은 “SQL만 잘 짜는 것”이 아니라,
재현 가능한 분석 파이프라인을 만드는 방식 자체였습니다.

ELT에서 T(Transformation)를 팀 단위로 운영하는 감각을 제대로 익힌 한 주였습니다.

#DataEngineering #dbt #AnalyticsEngineering #ELT #BigQuery #DataTalksClub

---

## 2) dbt 모델 레이어링(staging/intermediate/mart)
이번에 가장 크게 체감한 건 모델 레이어링의 힘이었습니다.

- `staging`: 원본 정리(타입 캐스팅, 컬럼명 표준화)
- `intermediate`: 비즈니스 로직 전 단계 결합/가공
- `mart`: 분석/리포팅 목적의 최종 테이블

장점:
- 디버깅이 쉬워짐
- 재사용성이 올라감
- 변경 영향 범위 추적이 쉬움

처음엔 파일이 많아 보였는데,
결국 유지보수 비용을 크게 줄여주는 구조였습니다.

#dbt #DataModeling #AnalyticsEngineering #SQL

---

## 3) `ref()`와 Lineage로 의존성 관리하기
`ref()`를 왜 써야 하는지 이번에 확실히 이해했습니다.

- 하드코딩된 테이블명 대신 `{{ ref('model_name') }}` 사용
- 모델 간 의존성이 자동으로 그래프화됨
- 실행 순서가 자동 해결됨

덕분에 특정 모델만 실행할 때도
upstream/downstream 범위를 유연하게 제어할 수 있었습니다.

“모델이 많아질수록 ref의 가치가 커진다”는 걸 직접 체감했습니다.

#dbt #Lineage #DataEngineering #Analytics

---

## 4) dbt Test로 데이터 품질 실패를 빨리 잡기
`accepted_values` 테스트 실습이 특히 좋았습니다.

예: `payment_type` 허용값을 [1,2,3,4,5]로 정의
→ 데이터에 6이 들어오면 테스트 실패

좋았던 포인트:
- 이상값 유입을 배치 단계에서 즉시 탐지
- “문제 1건”도 배포 전에 차단 가능
- 실패 SQL(compiled)까지 확인해서 원인 추적 가능

테스트 없는 파이프라인은 결국 늦게 터진다는 걸 다시 확인했습니다.

#DataQuality #dbt #Testing #AnalyticsEngineering

---

## 5) 월별 매출 데이터마트 만들기 (`fct_monthly_zone_revenue`)
리포팅 목적의 mart를 직접 만들면서,
집계 기준 설계가 결과 해석에 얼마나 중요한지 배웠습니다.

핵심 축:
- `pickup_zone`
- `revenue_month`
- `service_type`

핵심 지표:
- `revenue_monthly_total_amount`
- `total_monthly_trips`
- 평균 승객수/거리

결국 좋은 데이터마트는 “조회 성능”보다
“질문에 바로 답할 수 있는 구조”가 핵심이라는 걸 느꼈습니다.

#DataMart #BigQuery #dbt #BusinessIntelligence

---

## 6) 실무형 디버깅 포인트 정리 (이번에 실제로 겪은 것)
이번 과제에서 겪은 대표 이슈:
- `source()` 이름 불일치 (`raw` vs `raw_data`)
- model selector 오타 (`--select fct_monthly_zone_revenue.sql`)
- 컬럼 오타 (`service_typeb`)
- `LIMIT 1` + `ORDER BY` 없음으로 비결정적 결과

배운 점:
- SQL 실력만큼 네이밍 일관성이 중요
- “작동 안 함”의 대부분은 작은 불일치에서 시작
- 에러 로그의 compiled SQL 경로를 보는 습관이 강력함

#Debugging #dbt #DataEngineering #ETL

---

## 7) 이번 주 회고: SQL에서 Analytics Engineering으로
이번 주를 한 줄로 요약하면:
“쿼리를 잘 짜는 것”에서 “데이터 제품을 설계/운영하는 것”으로 시야가 넓어진 주.

앞으로 보완할 것:
- 테스트 커버리지 더 확장하기
- 문서화(`schema.yml`) 더 촘촘히 작성하기
- 모델 naming convention 팀 룰화

다음 모듈에서도
재현성, 테스트, 운영 관점으로 계속 학습해보겠습니다.

#LearningInPublic #DataEngineeringZoomcamp #dbt #AnalyticsEngineering

---

## 8) English Version - Module 4 Wrap-up
🚀 Completed Module 4 of Data Engineering Zoomcamp: Analytics Engineering with dbt

This week, I worked on transforming NYC Taxi raw data into analytics-ready models using dbt.

What I learned:
- How to structure models across `staging`, `intermediate`, and `marts`
- How to use `ref()` for dependency management and lineage
- The difference between `dbt run`, `dbt test`, and `dbt build`
- How generic tests (`not_null`, `unique`, `accepted_values`) catch data quality issues early
- How to build a monthly revenue mart for zone-level analysis

Big takeaway:
Analytics engineering is not just writing SQL. It is about building reliable, testable, and maintainable data transformation systems.

#DataEngineering #dbt #AnalyticsEngineering #ELT #BigQuery #LearningInPublic
