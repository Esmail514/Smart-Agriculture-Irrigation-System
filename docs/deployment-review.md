# مراجعة التشغيل والنشر — 25 أكتوبر 2026

أُصلحت عيوب تمنع تشغيل نسخة جديدة من المشروع رغم نجاح اختبارات منطق البيانات:

- Airflow يستخدم LocalExecutor بشكل صريح؛ لا يحتاج Redis أو Celery workers. تُبنى صورته مع Docker SDK بدلاً من الاعتماد على مكتبة غير مضمونة في الصورة الأصلية.
- تهيئة Airflow تتحقق من نجاح ترحيل قاعدة البيانات ومن وجود المسؤول. فشل الترحيل/إنشاء المستخدم يُرجع exit غير صفري، والحساب الموجود يحتفظ بكلمة مروره. لا تُطبع كلمة المرور في سجل الخطأ.
- صورة Spark تثبت اعتمادات weather/ML وقت البناء. أُزيل تثبيت pip أثناء تشغيل المهام. اعتمادات Spark مستقلة في `requirements-spark.txt` وتحافظ على NumPy1.x المناسب لمكتبات هذه النسخة.
- أُزيل قتل جميع streaming jobs كل خمس دقائق. Worker يحتاج أربعة cores على الأقل لتشغيل التطبيقات الأربعة، وكل تطبيق محصور في core واحد. مهام weather وML المحدودة تعمل بـ`local[1]` داخل spark-master حتى لا تنتظر خلف streams لا تنتهي. master/worker لكل منهما سقف Docker قدره4GB؛ يحتاج المضيف موارد كافية، وتظل مراجعة الاستهلاك الفعلي مطلوبة.
- إطلاق stream مقفل بواسطة flock لمنع تشغيل نسختين مع طلبات متزامنة. Bash وDocker SDK يعيدان فشل المهمة إلى Airflow، ويُغلق عميل Docker حتى عند الفشل. فحوص DAG تثبت وجود العملية فقط، ولا تثبت تقدم البيانات.
- runner ينتظر الخدمات المختارة فعلياً عبر profiles، ويتحقق من exit code لعمليات init؛ لا يعتبر init الفاشل ناجحاً لأنه exited. simulator حاوية اختيارية، وليس عملية خلفية جديدة على الجهاز مع كل تشغيل. إيقاف المشروع يشمل profiles ويحافظ على volumes.
- MinIO init يحترم اسم `LAKEHOUSE_BUCKET` ويتوقف عند فشل إنشاء/الوصول إلى bucket. PostgreSQL init يحترم `POSTGRES_AIRFLOW_DB` مع اقتباس اسم قاعدة البيانات؛ ينطبق ذلك على volume جديد، ولا يغيّر بيانات موجودة تلقائياً.
- الترحيل إلى ClickHouse يرفض HTTP redirects ويتجاوز إعداد proxy خارجي للاتصال المباشر بالعنوان المحدد. النسخ القديمة من الجداول تُحتفظ بها؛ لا تحذف volumes.

## التشغيل

من مجلد Pipeline، بعد إعداد `.env`:

```powershell
# الصور الجديدة تحتاج إعادة بناء؛ لا تستخدم SkipBuild للمرة الأولى بعد التحديث.
.\scripts\run-pipeline.ps1
# واجهات المشرف والمراقبة الاختيارية:
.\scripts\run-pipeline.ps1 -Monitoring -Tools
# تشغيل الإنتاج دون بيانات simulator:
.\scripts\run-pipeline.ps1 -NoSimulator
.\scripts\stop-pipeline.ps1
```

تهيئة مسؤول التطبيق وترحيل application schema في [دليل backend](backend-application.md). تشغيل DAG يعني تقديم طلب تشغيل، وليس إثبات انتهاء المعالجة. أوقف أي simulator قديم على الجهاز قبل اعتماد الحاوية لتجنب مضاعفة البيانات. الملفات المرجعية للمستشعرات والأبعاد تُجمع في snapshot ثابت عند بدء stream؛ بعد تعديل catalog/rules أعد تشغيل streams في فترة صيانة. الحد الافتراضي `METADATA_SNAPSHOT_MAX_ROWS=100000` يحمي ذاكرة driver، وتجاوزه يوقف المهمة بدلاً من جمع أبعاد غير محدودة.

## Kafka ووقت قبول الرسالة

broker الجديد وtopic المستشعر الجديد يستخدمان `LogAppendTime` ليصبح timestamp الذي يراه Bronze وقت وصول الرسالة إلى Kafka. الاسم الافتراضي `agri.sensors.raw`. إنشاء topic بـ`--if-not-exists` **لا يعدّل إعداد topic موجود**؛ نفّذ هذا التغيير صراحة على topic الفعلي عند ترقية نظام موجود:

```powershell
# استبدل agri.sensors.raw بقيمة KAFKA_TOPIC_SENSORS إذا كانت مختلفة.
docker compose exec kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka:9092 --entity-type topics --entity-name agri.sensors.raw --alter --add-config message.timestamp.type=LogAppendTime
docker compose exec kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka:9092 --entity-type topics --entity-name agri.sensors.raw --describe
```

لا يفترض هذا التغيير أن سجل الرسائل القديمة أُعيدت كتابة timestamps الخاصة به؛ الرسائل السابقة تحتفظ بطوابعها القديمة.

## التحقق وحدوده

اختبارات التنفيذ تستبدل Docker وspark-submit ببدائل صغيرة وتختبر فشل init، صون حساب المسؤول، انتشار exit code، إغلاق Docker client، readiness لفشل init، وتشغيل batch المحدود. اختبارات Compose/monitoring تفحص profiles والمصادر والمسارات والتخطيط. فحص Bash وPowerShell نحوي؛ لا يعادل بناء الصورة أو تشغيل Spark فعلياً.

بدأ Docker Desktop Linux لاحقاً في الجلسة (Engine29.7.2). فحص `docker compose ps -a` أكد عدم وجود حاويات لهذا المشروع، ولم تُطلق المجموعة أو تُعدّل قواعد بيانات المستخدم أو volumes. بُنيت صورة Airflow الجديدة فعلياً، وأثبت DagBag داخل حاوية معزولة قراءة DAGs الثلاثة دون import errors مع DockerSDK7.1.0 وRequests2.32.5. وشُغلت تهيئة Airflow مرتين ضد SQLite مؤقت داخل حاوية قابلة للإزالة ونجحت دون تكرار الحساب؛ ليس ذلك ترحيلاً لقاعدة المستخدم. وأثبت promtool داخل صورة Prometheus صلاحية config وقواعد التنبيه التسع ونجاح اختبارات سيناريوهات التنبيه وصحة23 استعلام PromQL في Grafana. أُوقف اختبار بناء Spark بعد تجاوز وقت التنزيل العملي أثناء جلب اعتمادات PyPI؛ ولم يكتمل بناء API/control بسبب بطء تنزيل طبقات Pythonbase. لم يثبت خطأ في source بسبب ذلك، ولا يُسجّل أي من هذه الصور الثلاث كصورة تم بناؤها بنجاح. لا يُعد فحص الصور المعزول إثباتاً لتكامل المجموعة كاملة. أوامر التشغيل والتحقق بعد إتمام بناء الصور:

```powershell
docker compose --profile monitoring --profile tools --profile simulation config --quiet
docker compose build
docker compose up -d
docker compose exec airflow-scheduler airflow dags list-import-errors
docker compose ps -a
```

### إعادة محاولة بناء الصور في متابعة المراجعة

في المتابعة تعذر بدء إعادة بناء `api` و`control-agent` و`spark-master`: لم يعد Docker Desktop يوفر pipe الخاص بـ`dockerDesktopLinuxEngine`. سجل المضيف `com.docker.backend.exe.log` يحدد فشل بدء Ingest server بسبب تعذر الوصول إلى `C:/Users/Esmail/AppData/Local/Docker/run/sailor-ingest.sock` لإزالته (`The file cannot be accessed by the system`). خدمة `com.docker.service` متوقفة، ومحاولة بدء الخدمة رفضها نظام Windows بسبب صلاحيات فتح الخدمة. جُرب أمر إعادة التشغيل المدعوم `docker desktop restart --timeout 60` مرة واحدة، لكنه ظل معلقاً دون استعادة Engine؛ أُوقفت عملية CLI الخاصة بالمحاولة بعد الانتظار المحدود. لم يُحذف socket ولم تُعد ضبط Docker أو توزيعات WSL أو volumes. تبقى نتائج اختبارات الحاويات السابقة صحيحة، لكن الصور الثلاث لم يُتحقق من بنائها في هذه المتابعة. يلزم استعادة Docker Desktop أولاً، ثم تنفيذ أوامر البناء والتحقق أعلاه.

المراجع: [Airflow entrypoint](https://airflow.apache.org/docs/docker-stack/entrypoint.html)، [Spark standalone scheduling](https://spark.apache.org/docs/3.5.6/spark-standalone.html)، [Kafka message timestamps](https://kafka.apache.org/37/generated/topic_config.html).
