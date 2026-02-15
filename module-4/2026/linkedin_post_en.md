I summarized what a **Data Analytics Engineer** actually does in real projects. 📌

- 🛠️ Build reliable transformation workflows with dbt
- 🧱 Turn raw data into analytics-ready models
- 🧭 Design data layers: `raw → staging → mart`
- ✅ Ensure data quality with tests (`not_null`, `unique`, `accepted_values`)
- 📊 Support BI/reporting with clear, reusable marts

The key idea: 💡
- 🔗 Analytics Engineers connect **data engineering** and **analytics**
- 📐 They focus on trustworthy datasets, metric consistency, and modeling standards
- ☁️ In BigQuery, this often means:
  - **project** for environment separation
  - **dataset** for layer/domain organization

Typical dbt mapping: 🗺️
- `source()` = raw/source table reference
- `ref()` = transformed model dependency
- `database` = project
- `schema` = dataset

For personal projects, starting with  🚀
**1 project + 3 datasets (raw/staging/mart)**  
is a practical setup to learn the role end-to-end.

Here’s my homework solution: https://github.com/kkh1902/DEZoomcamp-homework/blob/main/module-4/2026/homework.md

Anyone else currently learning data engineering? 🙋

You can join here: https://github.com/DataTalksClub/data-engineering-zoomcamp/

#DataEngineering #AnalyticsEngineering #dbt #BigQuery #ELT #DataTalksClub  
@Alexey Grigorev @DataTalksClub

---

## Version 2
I summarized why **Analytics Engineering** matters for team collaboration. 🤝

- 📚 Shared model definitions reduce confusion across teams
- 🧩 Layered models (`staging → intermediate → marts`) improve reuse
- 🧪 Built-in tests make data contracts explicit
- 🧾 Documentation in YAML keeps business meaning close to the code

The result: analysts, engineers, and stakeholders can work from the same trusted models.

Here’s my homework solution: https://github.com/kkh1902/DEZoomcamp-homework/blob/main/module-4/2026/homework.md

Anyone else currently learning data engineering? 🙋

You can join here: https://github.com/DataTalksClub/data-engineering-zoomcamp/

#DataEngineering #AnalyticsEngineering #dbt #DataModeling #BigQuery #DataTalksClub  
@Alexey Grigorev @DataTalksClub

---

## Version 3
This week I focused on the **operational side** of dbt workflows. ⚙️

- 🚦 Use `dbt build` when you want model + test confidence
- 🎯 Use selectors (`-s`, `+model+`, `path:`) to iterate faster
- 🔁 Use `--target` to separate dev and prod runs safely
- 🧯 Use tests to catch bad data before it reaches dashboards

Small command habits made a big difference in reliability.

Here’s my homework solution: https://github.com/kkh1902/DEZoomcamp-homework/blob/main/module-4/2026/homework.md

Anyone else currently learning data engineering? 🙋

You can join here: https://github.com/DataTalksClub/data-engineering-zoomcamp/

#DataEngineering #dbt #DataOps #AnalyticsEngineering #ELT #DataTalksClub  
@Alexey Grigorev @DataTalksClub

---

## Version 4
I wrote down the background of why **dbt** became so important in modern data teams. 🧠

Before dbt, many teams had this gap:
- 🚚 Data pipelines loaded raw data into warehouses
- ❓ Business logic lived in scattered SQL files and BI tools
- 🔁 Transformations were hard to version, test, and review

As cloud warehouses became the standard, teams needed a better way to manage the **T in ELT**.

That is where dbt helped:
- 🧱 SQL-first modeling with clear structure
- 🌐 Dependency management with `ref()`
- ✅ Built-in testing and documentation
- 🤝 Git-based collaboration for analytics code

Big takeaway: dbt turned ad-hoc SQL into an engineering workflow.

Here’s my homework solution: https://github.com/kkh1902/DEZoomcamp-homework/blob/main/module-4/2026/homework.md

Anyone else currently learning data engineering? 🙋

You can join here: https://github.com/DataTalksClub/data-engineering-zoomcamp/

#DataEngineering #dbt #AnalyticsEngineering #ELT #BigQuery #DataTalksClub  
@Alexey Grigorev @DataTalksClub

---

## Version 5
I wanted to share one practical lesson from Module 4: **most dbt failures are small mismatches, not big problems**. 🔍

What broke during my homework:
- ⚠️ `source()` name mismatch (`raw` vs `raw_data`) caused compilation errors
- ⚠️ Selector/column typos caused runs to fail (`.sql` selector, `service_typeb` typo)
- ⚠️ Data tests failed as expected (`accepted_values`, `unique`) after inserting bad records

What I learned:
- ✅ Keep naming conventions consistent
- ✅ Read compiled SQL paths in error logs
- ✅ Use test failures as feedback, not as blockers

Debugging dbt felt much easier once I treated errors as part of the workflow.

Here’s my homework solution: https://github.com/kkh1902/DEZoomcamp-homework/blob/main/module-4/2026/homework.md

Anyone else currently learning data engineering? 🙋

You can join here: https://github.com/DataTalksClub/data-engineering-zoomcamp/

#DataEngineering #dbt #Debugging #DataQuality #AnalyticsEngineering #DataTalksClub  
@Alexey Grigorev @DataTalksClub
