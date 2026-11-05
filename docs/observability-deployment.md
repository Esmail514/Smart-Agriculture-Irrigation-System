# التشغيل والمراقبة

الخدمات الأساسية تعمل عبر `docker compose up -d --build`: PostgreSQL، Kafka، MinIO، Spark master/worker، ClickHouse، Airflow، وAPI التطبيق. واجهات Kafka UI وSpark History اختيارية ضمن profile `tools`. Grafana وPrometheus ومصدّرات Kafka/PostgreSQL وفحوص الشبكة اختيارية ضمن profile `monitoring`.

```powershell
# من مجلد Pipeline
docker compose config --quiet
docker compose up -d --build
docker compose --profile monitoring up -d
docker compose --profile tools up -d
```

تغيير profile لا يوقف الحاويات التي تعمل مسبقاً. لإيقاف الواجهات الاختيارية الموجودة فقط:

```powershell
docker compose --profile tools stop kafka-ui spark-history
```

لا تحذف volumes: تحتوي بيانات PostgreSQL وClickHouse وبحيرة البيانات. يحتاج المشروع Docker Desktop يعمل؛ تعديل الملفات لا يطبق التغييرات على الحاويات تلقائياً. منافذ Spark داخل `infra/prometheus/prometheus.yml` تطابق الافتراضات `8090` و`8091` في `.env.example`؛ عند تعديل منافذ Spark عدّل أهداف الفحص كذلك.

استخدم حسابات المستخدمين وعضوية المزارع؛ اضبط `CORS_ORIGINS`، وللتوافق القديم فقط `API_TOKEN` و`API_FARM_IDS` في `.env` بحسب مستخدمي التطبيق والمزارع المسموحة وموقع الويب. API يعمل على المنفذ `8000` ويقرأ PostgreSQL وClickHouse عبر شبكة Docker. أي endpoint يعرض بيانات مستخدم يجب أن يلتزم بنطاق المزارع المحدد؛ فحص `/health` مخصص لتوفر الخدمة. API يوفر تسجيل دخول وصلاحيات ولوحات لكل مزرعة وتحكماً إدارياً؛ أوامر الري مسجلة في queue وتحتاج منفّذاً متصلاً بالأجهزة. خطوات إنشاء المسؤول موثقة في [دليل backend](backend-application.md).

## لوحات التحليل للمشرف

| لوحة Grafana | ما تعرضه | مصدر البيانات |
|---|---|---|
| Overview | عدد المزارع والمناطق والمستشعرات، حالة رطوبة المناطق، آخر التنبيهات، مواقع المزارع | ClickHouse + PostgreSQL |
| Soil Analytics | رطوبة وحرارة التربة والهواء، احتياج الري، تاريخ المنطقة، الحدود المناسبة للمحصول | ClickHouse + PostgreSQL |
| Weather & Irrigation | طقس المزرعة، المطر، الرياح بوحدة m/s، قرارات الري بحسب الطقس | ClickHouse |
| AI Recommendations | عدد التوصيات، تصنيف الجفاف الحالي، إصدار النموذج، ثقة التنبؤ وسجل المناطق | ClickHouse |
| Platform Operations | توفر الخدمات، حداثة البيانات، التخزين، تدفق Kafka، مؤشرات قواعد البيانات والتنبيهات | Prometheus + ClickHouse |

بطاقات الحالة الحالية تتجاهل بيانات المناطق الأقدم من 10 دقائق، وتعرض جداول الحالة عمر الملاحظة. عدد المستشعرات في ملخص المزرعة لا يثبت اتصال كل مستشعر؛ التطبيق يفحص بياناته الفردية. بطاقات عمق الري تعرض متوسط mm ولا تجمع أعماق مناطق مختلفة على أنها حجم ماء. المطر متوسط لكل مزرعة في الفترة المختارة، وهو تقدير من ملاحظات ساعة واحدة لكل ساعة UTC، لا قراءة عداد أمطار مستمر.

الجداول `*_latest` تعرض الحالة الحالية حتى عند تغيير الفترة الزمنية. التاريخ، الطقس، التنبيهات وML تستخدم الفترة المختارة في Grafana. اختيار جميع المزارع/المناطق يستخدم `IN` مع اقتباس القيم؛ أسماء تحتوي `%` أو `_` لا تتحول إلى wildcard. استخدم تطبيق المستخدم وAPI لتقييد بيانات المزرعة للمستخدم؛ Grafana المزوّد بهذه الملفات لوحة للمشرف، ولا يمثل حدّاً لعزل المستخدمين.

الاحتياج الموصى به بـmm ليس استهلاك ماء فعلياً؛ قياس الاستهلاك/التوفير يتطلب سجلات تشغيل المضخة أو عداد تدفق ومساحة المنطقة. ثقة ML ليست دقة النموذج، ولا يجب عرضها على أنها دقة؛ تقييم الدقة يحتاج نتائج ري مؤكدة وبيانات اختبار محفوظة.

## مقاييس النظام الموجودة

| المقياس | المصدر / معنى المؤشر |
|---|---|
| `up` | نجاح جلب المقاييس؛ فشل exporter يختلف عن فشل قاعدة البيانات |
| `probe_success`, `probe_duration_seconds` | HTTP لـSpark/Airflow/API وTCP لـKafka/PostgreSQL |
| `kafka_topic_partition_current_offset` | نمو رسائل الموضوع؛ تقدير نمو offset باستخدام `deriv` لأنه Gauge؛ تقليم الموضوع أو إعادة إنشائه قد يشوه التقدير |
| `kafka_consumergroup_lag` | تراكم الرسائل للمجموعات التي تحفظ offsets في Kafka |
| `pg_up`, `pg_stat_activity_count` | اتصال PostgreSQL وجلساته النشطة |
| `ClickHouseProfileEvents_Query`, `ClickHouseProfileEvents_InsertedRows`, `ClickHouseMetrics_MemoryTracking` | معدل الاستعلامات والإدخال وذاكرة ClickHouse |
| `minio_s3_traffic_received_bytes_total`, `minio_s3_traffic_sent_bytes_total` | حركة التخزين |
| `minio_cluster_capacity_usable_free_bytes` | مساحة MinIO المتاحة |
| `ALERTS` | التنبيهات النشطة من قواعد Prometheus |

Spark Structured Streaming يحفظ offsets في checkpoint وقد لا ينشئ committed consumer group؛ عندها لوحة lag تعرض No telemetry. استخدم تدفق offsets مع حداثة Gold كإشارة للتقدم؛ لا تفسّر غياب lag بأنه صفر. فحص Airflow `/health` يثبت استجابة HTTP فقط: نتيجة scheduler/database موجودة في جسم الرد ولا يقيّمها فحص HTTP العام. تراقب Docker healthcheck عملية scheduler نفسها. عدم وجود تنبيهات ري لا يعني توقف Kafka.

لا تغطي هذه النسخة CPU/RAM لكل حاوية أو مدّة DAG والمهام أو عدد الصفوف المقبولة/المرفوضة لكل طبقة أو drift للنموذج. إضافتها تتطلب cAdvisor على مضيف Linux، StatsD exporter لـAirflow، وinstrumentation داخل jobs/ML؛ لا توجد لوحات تدّعي هذه المقاييس قبل توفيرها.

قواعد التنبيه الحالية: زيادة أخطاء API عن 5% وp95 عن 3 ثوانٍ بشرط وجود أكثر من 20 طلباً خلال 5 دقائق، فشل تقييم قواعد Prometheus أو إعادة تحميل إعداده، تعذر جمع المقاييس، فشل endpoint، انقطاع PostgreSQL، lag يتجاوز 10,000 رسالة لمدة 10 دقائق، وانخفاض مساحة MinIO إلى أقل من 15%. اضبط حدود lag بحسب معدل simulator. القواعد تظهر في Prometheus/Grafana؛ إرسال البريد/الإشعارات يتطلب Alertmanager وتكوين جهة الإرسال.

## تحسينات التشغيل والتحقق

Grafana وPrometheus مرتبطان بـ`127.0.0.1` فقط؛ يمكن فتحهما محلياً على المنافذ 3000 و9090. وصول المشرف عن بعد يحتاج reverse proxy مؤمّناً أو SSH tunnel. أزيلت واجهة Prometheus الإدارية التي تسمح بحذف بيانات TSDB؛ إعادة التحميل عبر lifecycle تبقى متاحة من المضيف المحلي. هذه اللوحات تعرض بيانات جميع المزارع للمشرف، ولا تُضمّن بحساب Grafana عام داخل تطبيق المستخدم.

لوحة العمليات توفر فلاتر Kafka topic وAPI route والخدمة المفحوصة؛ روابط اللوحات تحتفظ بالفترة والاختيارات. نسبة أخطاء API تعرض صفراً فقط عندما توجد مقاييس طلبات للمسار، وتظل مجهولة عند غياب القياسات أو المرور. جدول التخزين يحافظ على أرقام bytes/rows لتسهيل الفرز؛ عدد الصفوف فيه فيزيائي وقد يشمل صفوف إعادة المحاولة قبل الدمج. جدول حداثة datasets يعرض أحدث صف فقط بحد 10 دقائق للمستشعرات و120 للطقس؛ لا يثبت حداثة كل مزرعة، ووقت تنبؤ ML لا يثبت حداثة features (يتحقق منها API).

## التحقق بعد التشغيل

```powershell
docker compose --profile monitoring exec prometheus promtool check config /etc/prometheus/prometheus.yml
docker compose --profile monitoring exec prometheus promtool check rules /etc/prometheus/rules/agri-alerts.yml
# اختبارات سيناريوهات التنبيه، باستخدام promtool المحلي:
promtool test rules infra/prometheus/tests/agri-alerts.test.yml
docker compose --profile monitoring ps
```

افتح Prometheus `/targets` للتحقق من أن الأهداف تعمل، ثم Grafana واستعمل Query Inspector للتحقق من استعلامات ClickHouse مع مزرعة واحدة وجميع المزارع وفترات مختلفة. الاختبارات المحلية تتحقق من مراجع الجداول والمصادر والأهداف وتناسق profiles؛ لا تحل محل التحقق الحي.

المراجع الرسمية: [Kafka exporter](https://github.com/danielqsj/kafka_exporter)، [Blackbox exporter](https://github.com/prometheus/blackbox_exporter/blob/master/CONFIGURATION.md)، [PostgreSQL exporter](https://github.com/prometheus-community/postgres_exporter).

مراجع الاستعلامات: [Prometheus gauge functions](https://prometheus.io/docs/prometheus/latest/querying/functions/)، [Grafana variable syntax](https://grafana.com/docs/grafana/latest/visualizations/dashboards/variables/variable-syntax/).
