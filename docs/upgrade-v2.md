# تفعيل النسخة المعدّلة

هذه التعديلات محلية. Docker Desktop لم يكن يعمل أثناء المراجعة، لذلك لم تُغيّر الجداول أو الحاويات الحية ولم تُحذف بيانات قديمة. الاختبارات المحلية لا تعوّض تشغيل Spark/JDBC/Grafana فعليًا.

1. احتفظ بنسخة احتياطية من PostgreSQL وClickHouse وMinIO. أوقف المحاكي وSpark writers وDAGs المجدولة قبل ترحيل الجداول. أبقِ قواعد البيانات متاحة. لا تحذف volumes ولا checkpoints.
2. شغّل سكربت ClickHouse من `Pipeline` باستخدام Python يحتوي requests. اقرأ credentials من البيئة المحلية، دون كتابتها في command line. الـURL الافتراضي `http://localhost:8123`:

```powershell
venv\Scripts\python.exe scripts\migrate_contract_v2.py --url http://localhost:8123
```

السكربت يضيف الجداول والحقائق والأبعاد والعقود nullable، ويعيد بناء جداول history/alerts/ML بمحركات إزالة تكرار ثم EXCHANGE TABLES. يحتفظ بالنسخ الأصلية بأسماء `*_legacy_v1`. يدعم استئناف فشل بين exchange وإعادة تسمية النسخة. لا تشغّله مع writers نشطين. `CREATE TABLE IF NOT EXISTS` وحده لا يغيّر schema أو engine لقاعدة موجودة.

3. اضبط إعدادات API في `.env`: رمز طويل عشوائي في API_TOKEN ونطاق واضح في API_FARM_IDS. النطاق الفارغ يمنح صفر مزارع. للمستخدمين المتعددين استخدم API_ACCOUNTS_JSON، مثل بنية `[{"token":"...","id":"user-1","full_name":"...","email":"...","farm_ids":[1,2]}]`. خزّن الرموز محليًا ولا تضمّنها في build الويب. API الحالي للقراءة؛ التسجيل واستعادة الحسابات والتحكم بالصمامات يحتاجان خدمات مستقلة.
4. شغّل Docker Desktop ثم الخدمات الأساسية. أدوات الإدارة والمراقبة اختيارية:

```powershell
docker compose config --quiet
docker compose up -d --build
docker compose --profile monitoring up -d
# لإيقاف الواجهات الإضافية الموجودة فقط
docker compose --profile tools stop kafka-ui spark-history
```

5. أعد تشغيل DAGs والمحاكي. يمكن تشغيله كـmodule من Pipeline بـ`venv\Scripts\python.exe -m src.ingestion.sensor_simulator.simulator` بعد ضبط PostgreSQL/Kafka لعناوين المضيف. SEND_INTERVAL موجب، ANOMALY_RATE بين 0 و1، وSIMULATE_IRRIGATION=false افتراضيًا. إظهار تحسن رطوبة اصطناعي بعد الري يتطلب تفعيل الخيار صراحة؛ لا يمثل تشغيل مضخة.
6. شغّل واجهة المستخدم من App:

```powershell
flutter run -d chrome --web-port=8080 --dart-define=API_BASE_URL=http://localhost:8000
```

اضبط CORS_ORIGINS على عنوان الواجهة بالضبط. للهواتف استعمل عنوان API القابل للوصول من الجهاز؛ localhost يشير للهاتف نفسه. افتح صفحة تسجيل الدخول وأدخل token المستخدم. `DEMO_MODE=true` فقط عند اختيار التجربة صراحة. بناء الويب في `App/build/web`؛ نشره العام مع TLS وإعداد النطاق لم يُنفّذ.

## بيانات المرحلة السابقة

العقود الجديدة تستخدم sensor_data_v2 وweather_v2 ومجلدات Gold/checkpoints منفصلة. لم نخلط ملفات Parquet القديمة التي قد تحتوي أنواعًا مستنتجة مختلفة أو rejected records. مسارات النسخة السابقة باقية ولم تُنظَّف لأن MinIO غير متاح. Bronze الجديدة مقبولة فقط؛ القديمة تحتاج إعادة فحص batch قبل اعتمادها أو نقلها إلى archive خارج Bronze.

Bronze Sensor يبدأ من أقدم offsets ما زالت محفوظة في Kafka. ذلك لا يستعيد أحداثًا تجاوزت retention؛ إذا أردت كامل التاريخ يلزم revalidation/backfill من archive وفق العقد الحالي. لا تنقل Parquet القديم مباشرة إلى v2 ولا تعِد استخدام checkpoint قديم. قد تعرض ClickHouse تاريخًا قديمًا أثناء الانتقال، وتستبدَل latest snapshots مع أول دفعة ناجحة. ML القديم rule-derived ولا يصبح forecast صحيحًا نتيجة ترحيل الجدول.

## قبول التشغيل الحي

- `/health` يعيد 200، وtoken الصحيح يقرأ فقط المزارع المسموحة، وطلبات المزارع الأخرى تعيد 403.
- أرسل قراءة صحيحة وأخرى null/out-of-range/unknown sensor؛ تأكد من quarantine للأخيرة وعدم ظهورها في Bronze الجديدة أو UI.
- شغّل المراحل على بحيرة فارغة، ثم أوقف Gold/CH sync وسط دفعة وأعد تشغيلهما. تأكد من عدم فقد farms أو تكرار facts عبر FINAL ومن بقاء manifest السابق صالحًا.
- اختبر metadata جديدًا بعد إعادة تشغيل streams، وغياب قاعدة محصول، وحساسًا متوقفًا، وWeather HTTP429/500، والبيانات القديمة.
- راجع Prometheus targets وpromtool وGrafana Query Inspector للمزارع المفردة والجميع وتغيير الوقت. قائمة اللوحات والمقاييس وحدودها في `observability-deployment.md`.

تم التحقق محليًا من سلوك العقود وAPI والوحدات وML والواجهات وتهيئة Compose. التشغيل داخل Spark3.5/ClickHouse، الترحيل الفعلي، build Android/iOS، توصيل forecast/actuator، وإشعارات Alertmanager تبقى غير متحقق منها أو غير موصولة في هذه الجلسة.
